//! End-to-end load integrity — reconcile **source → file → warehouse** row counts before (and after) a load.

use crate::destination::gcs::GcsStore;
use crate::manifest::MANIFEST_MAX_BYTES;
use crate::manifest::census::{ManifestCensus, dedupe_by_run_id, ensure_single_generation};
use crate::manifest::{MANIFEST_FILENAME, ManifestStatus, PartStatus, RunManifest};
use anyhow::{Context, Result};

/// The reconciled row-count chain for one export's load, derived from the run manifests under its GCS prefix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadIntegrity {
    /// Rows the source held at extraction time, summed over the manifests that probed it.
    pub source_rows: Option<u64>,
    /// Rows written to files — the sum of the trustworthy manifests' `row_count`.
    pub file_rows: u64,
    /// How many run manifests contributed to the totals.
    pub manifests: usize,
}

impl LoadIntegrity {
    /// A one-line human summary of the chain, e.g.
    pub fn chain_prefix(&self) -> String {
        let src = self
            .source_rows
            .map_or_else(|| "?".to_string(), |n| n.to_string());
        format!("source {src} → files {}", self.file_rows)
    }
}

/// Fetch and parse every `manifest.json` under `gcs_prefix` (recursive), keeping each manifest's bucket-relative storage key.
#[allow(private_interfaces)]
pub fn fetch_manifests_keyed(
    store: &GcsStore,
    gcs_prefix: &str,
) -> Result<Vec<(String, RunManifest)>> {
    let (_, base) = crate::load::split_object_uri(gcs_prefix)?;
    let keys = list_manifest_keys(store, base)?;
    store
        .read_each_within(&keys, MANIFEST_MAX_BYTES, |key, body| {
            let bytes = body.map_err(|sz| {
                crate::error::coded(crate::error::codes::LOAD_STAGING_OBJECT_UNSAFE, format!(
                    "manifest {key} is at least {sz} bytes, over the {MANIFEST_MAX_BYTES}-byte cap — \
                     refusing to read a possibly-hostile manifest into memory (CWE-400)"
                ))
            })?;
            let m = serde_json::from_slice::<RunManifest>(&bytes)
                .with_context(|| format!("parsing manifest {key}"))?;
            Ok((key.to_string(), m))
        })
        .map(dedupe_by_run_id)
}

/// Is the object at `key` STILL a `running` marker, read now rather than from the census snapshot?
fn still_a_running_marker(store: &GcsStore, key: &str) -> bool {
    store.read(key).is_ok_and(|bytes| {
        serde_json::from_slice::<RunManifest>(&bytes)
            .is_ok_and(|m| m.status == ManifestStatus::Running)
    })
}

/// Full `gs://` URIs of the parquet to load for `new` (the not-yet-loaded run manifests), preferring each manifest's own parts over a blanket listing.
#[allow(private_interfaces)]
pub fn select_load_uris(
    store: &GcsStore,
    gcs_prefix: &str,
    new: &[(String, RunManifest)],
) -> Result<Vec<String>> {
    let (bucket, base) = crate::load::split_object_uri(gcs_prefix)?;
    let all_parquet: Vec<String> = store
        .list_files(base)?
        .into_iter()
        .filter(|k| k.ends_with(".parquet"))
        .collect();
    let missing = runs_with_missing_parts(new, &all_parquet);
    if !missing.is_empty() {
        crate::rivet_bail!(
            crate::error::codes::LOAD_RUN_NOT_LOADABLE,
            "run(s) {} declare parts that are no longer under {gcs_prefix} — refusing to load \
             rather than guess which files are theirs (the prefix may hold other runs' parts \
             or crash leftovers). Re-run the export, then load.",
            missing.join(", ")
        );
    }
    Ok(select_load_keys(new, &all_parquet)
        .into_iter()
        .map(|k| format!("{}://{bucket}/{k}", crate::load::scheme_of(gcs_prefix)))
        .collect())
}

/// The runs whose manifest declares parts of which none is present.
fn runs_with_missing_parts(new: &[(String, RunManifest)], all_parquet: &[String]) -> Vec<String> {
    let present: std::collections::HashSet<&str> = all_parquet.iter().map(String::as_str).collect();
    new.iter()
        .filter(|(key, m)| {
            m.committed_part_count() > 0
                && !resolve_parts(key, m).any(|p| present.contains(p.as_str()))
        })
        .map(|(_, m)| m.run_id.clone())
        .collect()
}

/// The bucket-relative COMMITTED part keys a manifest declares (a superseded repair original is neither loaded nor kept, so gc collects it once no run is active), each resolved against the manifest's own directory.
pub(crate) fn resolve_parts<'a>(
    manifest_key: &'a str,
    m: &'a RunManifest,
) -> impl Iterator<Item = String> + 'a {
    let dir = manifest_key.rsplit_once('/').map(|(d, _)| d).unwrap_or("");
    m.parts
        .iter()
        .filter(|p| p.status == PartStatus::Committed)
        .map(move |p| {
            if dir.is_empty() {
                p.path.clone()
            } else {
                format!("{dir}/{}", p.path)
            }
        })
}

/// Pure selection: which bucket-relative parquet keys to load for the given (not-yet-loaded) run manifests.
pub fn select_load_keys(new: &[(String, RunManifest)], all_parquet: &[String]) -> Vec<String> {
    use std::collections::BTreeSet;
    let present: std::collections::HashSet<&str> = all_parquet.iter().map(String::as_str).collect();
    let mut selected: BTreeSet<String> = BTreeSet::new();
    for (key, m) in new {
        let mut resolved_any = false;
        for full in resolve_parts(key, m) {
            if present.contains(full.as_str()) {
                selected.insert(full);
                resolved_any = true;
            }
        }
        if !resolved_any && m.committed_part_count() > 0 {
            return all_parquet.to_vec();
        }
    }
    selected.into_iter().collect()
}

/// Delete every `.parquet` under `gcs_prefix` that no **`Success`** manifest references.
#[allow(private_interfaces)]
pub fn gc_orphans(
    store: &GcsStore,
    gcs_prefix: &str,
    keyed: &[(String, RunManifest)],
    active: bool,
    dead_marker_run_ids: &std::collections::HashSet<String>,
) -> Result<(usize, u64)> {
    let (_bucket, base) = crate::load::split_object_uri(gcs_prefix)?;
    let census = ManifestCensus::new(keyed);
    let mut keep: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut terminal: std::collections::HashSet<String> = std::collections::HashSet::new();
    for run in census.runs() {
        let (key, m) = (run.key, run.manifest);
        match m.status {
            ManifestStatus::Success => keep.extend(resolve_parts(key, m)),
            ManifestStatus::Running => {}
            ManifestStatus::Failed | ManifestStatus::Interrupted => {
                if census.adopted_by_active_running(run) || !census.superseded(run) {
                    keep.extend(resolve_parts(key, m));
                } else {
                    terminal.extend(resolve_parts(key, m));
                }
            }
        }
    }
    let mut removed = 0usize;
    let mut removed_bytes = 0u64;
    for key in store.list_files(base)? {
        if !key.ends_with(".parquet") || keep.contains(&key) {
            continue;
        }
        if active && !terminal.contains(&key) {
            log::warn!(
                "gc_orphans: sparing unmanifested `{key}` — a run is active on this prefix \
                 (run-status ledger); it is GC'd once no run is active or it gets a manifest"
            );
            continue;
        }
        removed_bytes += store.stat_size(&key).unwrap_or(0);
        store.remove(&key)?;
        removed += 1;
    }
    for run in census.runs() {
        let dead_by_ledger = dead_marker_run_ids.contains(&run.manifest.run_id);
        if run.manifest.status == ManifestStatus::Running
            && (census.superseded(run) || dead_by_ledger)
            && still_a_running_marker(store, run.key)
        {
            removed_bytes += store.stat_size(run.key).unwrap_or(0);
            store.remove(run.key)?;
            removed += 1;
        }
    }
    Ok((removed, removed_bytes))
}

/// Is a LIVE run's `running` MARKER manifest present under the prefix.
pub fn has_active_running_manifest(keyed: &[(String, RunManifest)]) -> bool {
    ManifestCensus::new(keyed).active_running()
}

/// WHICH runs are still writing under the prefix, per the bucket markers.
pub fn active_running_run_ids(
    keyed: &[(String, RunManifest)],
) -> std::collections::HashSet<String> {
    ManifestCensus::new(keyed).active_running_ids()
}

/// Full/chunked loads care only about the LATEST snapshot.
pub fn latest_full(keyed: Vec<(String, RunManifest)>) -> Vec<(String, RunManifest)> {
    let picked: Vec<usize> = ManifestCensus::new(&keyed)
        .latest_generation()
        .iter()
        .map(|r| r.index)
        .collect();
    take_in_order(keyed, &picked)
}

/// [`latest_full`], refused unless the selection is ONE coherent split generation.
fn latest_single_generation(
    keyed: Vec<(String, RunManifest)>,
) -> Result<Vec<(String, RunManifest)>> {
    let sel = latest_full(keyed);
    ensure_single_generation(&ManifestCensus::new(&sel).runs().iter().collect::<Vec<_>>())?;
    Ok(sel)
}

/// Manifests keyed by their bucket key.
pub type Keyed = Vec<(String, RunManifest)>;

/// The baseline legs a base+buffer load writes (the newest generation, as a Full load picks it) and the run ids of the older legs that generation supersedes.
pub fn latest_baseline_generation(
    baseline: Vec<(String, RunManifest)>,
) -> Result<(Keyed, Vec<String>)> {
    let all: Vec<String> = baseline.iter().map(|(_, m)| m.run_id.clone()).collect();
    let sel = latest_single_generation(baseline)?;
    let superseded = all
        .into_iter()
        .filter(|id| sel.iter().all(|(_, m)| &m.run_id != id))
        .collect();
    Ok((sel, superseded))
}

/// Move the entries at `picked` out of `keyed`, in `picked`'s order.
fn take_in_order(
    keyed: Vec<(String, RunManifest)>,
    picked: &[usize],
) -> Vec<(String, RunManifest)> {
    let mut slots: Vec<Option<(String, RunManifest)>> = keyed.into_iter().map(Some).collect();
    picked
        .iter()
        .filter_map(|&i| slots.get_mut(i).and_then(Option::take))
        .collect()
}

/// Which run manifests to load for `mode`, given the ledger's already-`loaded` run_ids.
pub fn select_runs(
    keyed: Vec<(String, RunManifest)>,
    loaded: &std::collections::HashSet<String>,
    mode: crate::load::plan::LoadMode,
) -> Result<Vec<(String, RunManifest)>> {
    let keyed: Vec<(String, RunManifest)> = keyed
        .into_iter()
        .filter(|(_, m)| m.status == ManifestStatus::Success)
        .collect();
    match mode {
        crate::load::plan::LoadMode::Full => latest_single_generation(keyed),
        _ => Ok(keyed
            .into_iter()
            .filter(|(_, m)| !loaded.contains(&m.run_id))
            .collect()),
    }
}

/// Whether a FAILED read of the already-loaded set must stop the load.
pub fn ledger_read_failure_is_fatal(mode: crate::load::plan::LoadMode) -> bool {
    !matches!(mode, crate::load::plan::LoadMode::Full)
}

/// Bucket-relative keys of every run manifest under `base` (recursive).
fn list_manifest_keys(store: &GcsStore, base: &str) -> Result<Vec<String>> {
    let all: Vec<String> = store
        .list_files(base)?
        .into_iter()
        .filter(|k| is_manifest_key(k))
        .collect();
    Ok(all)
}

/// A listed key is a run manifest iff its final path segment is the canonical [`MANIFEST_FILENAME`] (`manifest.json`) or a run-unique copy (`manifest-<run_id>.json`).
fn is_manifest_key(key: &str) -> bool {
    let base = key.rsplit('/').next().unwrap_or("");
    base == MANIFEST_FILENAME || is_run_unique_manifest(base)
}

/// A per-run manifest copy: `manifest-<token>.json` (the sidecar the OSS sink writes alongside the canonical pointer so cross-run reconcile can sum it).
fn is_run_unique_manifest(base: &str) -> bool {
    crate::manifest::is_run_unique_manifest_name(base)
}

/// The manifests under a load's prefix without the runs of a CDC export nested inside it, and a one-line note naming the directories skipped (`None` when nothing was skipped).
pub fn own_manifests(
    keyed: Vec<(String, RunManifest)>,
    gcs_prefix: &str,
    export_name: &str,
    mode: crate::load::plan::LoadMode,
) -> Result<(Keyed, Option<String>)> {
    if mode == crate::load::plan::LoadMode::Cdc
        || seeded!(
            "nested-cdc-runs-loaded-by-batch",
            !gcs_prefix.is_empty(),
            false
        )
    {
        return Ok((keyed, None));
    }
    let (_, base) = crate::load::split_object_uri(gcs_prefix)?;
    let base = base.trim_end_matches('/');
    let dir = |k: &str| k.rsplit_once('/').map_or("", |(d, _)| d).to_string();
    let cdc_shaped: Vec<bool> = keyed
        .iter()
        .map(|(_, m)| {
            let family = crate::manifest::identity_family(m, &keyed);
            let leg_prefix = format!("{family}{}", crate::manifest::SNAPSHOT_LEG_INFIX);
            family == export_name && (m.mode == "cdc" || m.export_name.starts_with(&leg_prefix))
        })
        .collect();
    let batch_dirs: std::collections::BTreeSet<String> = keyed
        .iter()
        .zip(&cdc_shaped)
        .filter(|(_, cdc)| !**cdc)
        .map(|((k, _), _)| dir(k))
        .collect();
    let mut skipped: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    let mut own = Vec::new();
    for ((key, m), cdc) in keyed.into_iter().zip(cdc_shaped) {
        let d = dir(&key);
        if cdc && d != base && !batch_dirs.contains(&d) {
            skipped.insert(format!("{d}/"));
        } else {
            own.push((key, m));
        }
    }
    let note = (!skipped.is_empty()).then(|| {
        format!(
            "  load [{export_name}]: skipped the CDC runs under {} — a CDC export nested in \
             this batch export's prefix; its own `rivet load` loads them",
            skipped.into_iter().collect::<Vec<_>>().join(", ")
        )
    });
    Ok((own, note))
}

/// Refuse a load whose prefix holds MORE THAN ONE export's manifests.
pub fn ensure_single_export(keyed: &[(String, RunManifest)]) -> Result<()> {
    let mut names: std::collections::BTreeSet<&str> = keyed
        .iter()
        .map(|(_, m)| crate::manifest::identity_family(m, keyed))
        .filter(|n| !n.is_empty())
        .collect();
    if names.len() > 1 {
        let listed: Vec<&str> = std::mem::take(&mut names).into_iter().collect();
        crate::rivet_bail!(
            crate::error::codes::LOAD_SOURCES_SHARED,
            "the load prefix holds manifests from {} distinct exports ({}) — the load sums every \
             manifest under the prefix and cleanup wipes it recursively, so loading a shared \
             prefix would cross-contaminate the row count and could delete a sibling export's \
             parts. Give each export a DISTINCT destination prefix (or scope the load prefix to a \
             single export) and re-run.",
            listed.len(),
            listed.join(", ")
        );
    }
    ensure_single_source(keyed)
}

/// One family is not one EXPORT when two different databases are called the same thing.
fn ensure_single_source(keyed: &[(String, RunManifest)]) -> Result<()> {
    let sources: std::collections::BTreeSet<String> = keyed
        .iter()
        .map(|(_, m)| crate::manifest::identity_source(m))
        .filter(|s| !s.is_empty())
        .filter(|s| {
            if s.contains(':') {
                return true;
            }
            !keyed.iter().any(|(_, other)| {
                let o = crate::manifest::identity_source(other);
                o.len() > s.len() && o.starts_with(s.as_str()) && o.as_bytes()[s.len()] == b':'
            })
        })
        .collect();
    if sources.len() > 1 {
        let listed: Vec<&str> = sources.iter().map(String::as_str).collect();
        crate::rivet_bail!(
            crate::error::codes::LOAD_SOURCES_SHARED,
            "the load prefix holds manifests from {} DIFFERENT SOURCES ({}) under one export \
             name. The warehouse table is named from the source table, so loading this prefix \
             puts one source's rows in the table and leaves the other's unloaded — and a second \
             load REPLACES the first. Both commands report success while half the data never \
             arrives. Give each source its own destination prefix and its own export name, or \
             load them into separate target tables.",
            listed.len(),
            listed.join(", ")
        );
    }
    Ok(())
}

/// Reconcile a run's manifests into the authoritative expected warehouse row count, refusing to load anything that is not provably complete.
pub fn reconcile(manifests: &[RunManifest], allow_source_drift: bool) -> Result<LoadIntegrity> {
    if manifests.is_empty() {
        crate::rivet_bail!(
            crate::error::codes::LOAD_RUN_NOT_LOADABLE,
            "no `{MANIFEST_FILENAME}` found under the export prefix — refusing to load \
             unverified files. A rivet export writes a manifest on success; its absence \
             means the run never completed (or points at the wrong prefix)."
        );
    }

    let mut file_rows: u64 = 0;
    let mut source_rows: u64 = 0;
    let mut any_source = false;

    for m in manifests {
        if m.status != ManifestStatus::Success {
            crate::rivet_bail!(
                crate::error::codes::LOAD_RUN_NOT_LOADABLE,
                "manifest for run `{}` (export `{}`) is {:?}, not Success — refusing to load a \
                 partial export",
                m.run_id,
                m.export_name,
                m.status
            );
        }
        m.validate_self_consistency().map_err(|e| {
            crate::error::coded(crate::error::codes::LOAD_MANIFEST_INCONSISTENT, format!(
                "manifest for run `{}` (export `{}`) is internally inconsistent: {e} — refusing \
                 to load",
                m.run_id,
                m.export_name
            ))
        })?;

        let rows = u64::try_from(m.row_count).with_context(|| {
            format!(
                "manifest for run `{}` has a negative row_count ({})",
                m.run_id, m.row_count
            )
        })?;
        file_rows += rows;

        if let Some(src) = m
            .source
            .extraction
            .as_ref()
            .and_then(|x| x.source_row_count)
        {
            let src = u64::try_from(src).with_context(|| {
                format!(
                    "manifest for run `{}` has a negative source_row_count ({src})",
                    m.run_id
                )
            })?;
            any_source = true;
            source_rows += src;
            if src != rows {
                if allow_source_drift {
                    eprintln!(
                        "warning: source→file drift for run `{}` (export `{}`): source had {src} \
                         rows, extracted {rows} (allowed by `allow_source_drift: true`)",
                        m.run_id, m.export_name
                    );
                } else {
                    crate::rivet_bail!(
                        crate::error::codes::LOAD_SOURCE_FILE_MISMATCH,
                        "source→file mismatch for run `{}` (export `{}`): source had {src} rows \
                         but {rows} were extracted — the extract dropped {} row(s). Investigate \
                         before loading; to accept it, set `allow_source_drift: true` in this \
                         export's `load:` override (or the top-level `load:` block) and re-run \
                         `rivet load`.",
                        m.run_id,
                        m.export_name,
                        src.abs_diff(rows)
                    );
                }
            }
        }
    }

    Ok(LoadIntegrity {
        source_rows: any_source.then_some(source_rows),
        file_rows,
        manifests: manifests.len(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::{
        ExtractionMetadata, ManifestDestination, ManifestPart, ManifestSource, PartStatus,
    };

    /// A minimal `Success` manifest with one committed part of `rows` rows and, optionally, a probed `source_row_count`.
    fn manifest(run: &str, rows: i64, source: Option<i64>) -> RunManifest {
        RunManifest {
            split_window: None,
            checksum_render: None,
            row_hash: None,
            manifest_version: crate::manifest::MANIFEST_VERSION,
            run_id: run.into(),
            export_name: "orders".into(),
            export_family: String::new(),
            mode: "batch".into(),
            started_at: "t".into(),
            finished_at: "t".into(),
            status: ManifestStatus::Success,
            source: ManifestSource {
                engine: "pg".into(),
                schema: None,
                table: None,
                extraction: source.map(|n| ExtractionMetadata {
                    strategy: "full".into(),
                    cursor_column: None,
                    cursor_type: None,
                    cursor_low: None,
                    cursor_high: None,
                    source_row_count: Some(n),
                }),
            },
            destination: ManifestDestination {
                kind: "gcs".into(),
                uri: "gs://b/p".into(),
            },
            format: "parquet".into(),
            compression: "zstd".into(),
            schema_fingerprint: "xxh3:0".into(),
            row_count: rows,
            part_count: 1,
            parts: vec![ManifestPart {
                part_id: 0,
                path: "part-000000.parquet".into(),
                rows,
                size_bytes: 1,
                content_fingerprint: "xxh3:0".into(),
                content_md5: String::new(),
                status: PartStatus::Committed,
            }],
            column_checksums: None,
            checksum_key_column: None,
            nulled_cells: None,
        }
    }

    #[test]
    fn sums_file_and_source_rows_across_manifests() {
        let ms = vec![manifest("r1", 100, Some(100)), manifest("r2", 40, Some(40))];
        let got = reconcile(&ms, false).unwrap();
        assert_eq!(got.file_rows, 140);
        assert_eq!(got.source_rows, Some(140));
        assert_eq!(got.manifests, 2);
    }

    #[test]
    fn ensure_single_export_refuses_a_prefix_shared_by_two_exports() {
        let keyed = |m: RunManifest| ("gs://b/p/manifest-x.json".to_string(), m);
        let orders = keyed(manifest("r1", 10, None));
        let mut cust_m = manifest("r2", 10, None);
        cust_m.export_name = "customers".into();
        assert!(ensure_single_export(&[orders.clone(), keyed(cust_m)]).is_err());
        assert!(ensure_single_export(&[orders.clone(), keyed(manifest("r3", 5, None))]).is_ok());
        let mut legacy = manifest("r0", 3, None);
        legacy.export_name = String::new();
        assert!(ensure_single_export(&[orders, keyed(legacy)]).is_ok());
    }

    /// Two DIFFERENT databases, ONE export name, one prefix — refuse.
    #[test]
    fn ensure_single_export_refuses_two_different_sources_under_one_name() {
        let keyed = |m: RunManifest| ("gs://b/p/manifest-x.json".to_string(), m);
        let mut pg = manifest("r1", 300, None);
        pg.source.engine = "postgres".into();
        pg.source.table = Some("orders".into());
        let mut my = manifest("r2", 300, None);
        my.source.engine = "mysql".into();
        my.source.table = Some("orders".into());
        assert_eq!(
            pg.export_name, my.export_name,
            "the fixture must share the name"
        );

        let err = ensure_single_export(&[keyed(pg.clone()), keyed(my.clone())])
            .expect_err("two sources under one name must be refused, not silently half-loaded");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("DIFFERENT SOURCES") && msg.contains("postgres") && msg.contains("mysql"),
            "the refusal must NAME both sources — an operator cannot act on `something is \
             ambiguous`: {msg}"
        );

        let mut again = manifest("r3", 300, None);
        again.source.engine = "postgres".into();
        again.source.table = Some("orders".into());
        assert!(
            ensure_single_export(&[keyed(pg), keyed(again)]).is_ok(),
            "repeated runs of one export must not be refused — that is the normal load"
        );
    }

    /// A `:` in a collection/table name must not fold two DIFFERENT sources into one (bug hunt 2026-08-08).
    #[test]
    fn ensure_single_source_does_not_fold_two_colon_named_collections() {
        let keyed = |m: RunManifest| ("gs://b/p/manifest-x.json".to_string(), m);
        let mut a = manifest("r1", 10, None);
        a.source.engine = "mongo".into();
        a.source.table = Some("orders".into());
        let mut b = manifest("r2", 10, None);
        b.source.engine = "mongo".into();
        b.source.table = Some("orders:archive".into());
        b.export_name = a.export_name.clone();
        assert!(
            ensure_single_export(&[keyed(a.clone()), keyed(b.clone())]).is_err(),
            "two collections whose names share a `:` boundary are TWO sources"
        );
        let mut bare = manifest("r3", 10, None);
        bare.source.engine = "mongo".into();
        bare.source.table = None;
        bare.export_name = a.export_name.clone();
        assert!(
            ensure_single_export(&[keyed(a), keyed(bare)]).is_ok(),
            "a bare engine is still the same source, less detail"
        );
    }

    /// Regression (0.24.3, found on the CDC pilot).
    #[test]
    fn ensure_single_source_admits_the_batch_legs_unrecorded_table() {
        let keyed = |m: RunManifest| ("gs://b/p/manifest-x.json".to_string(), m);
        let mut drain = manifest("r1", 10, None);
        drain.source.engine = "mysql".into();
        drain.source.table = Some("orders".into());
        let mut snap = manifest("r2", 10, None);
        snap.source.engine = "mysql".into();
        snap.source.table = None;
        snap.export_name = drain.export_name.clone();
        assert!(
            ensure_single_export(&[keyed(drain.clone()), keyed(snap.clone())]).is_ok(),
            "the batch leg's unrecorded table is the SAME source, less detail"
        );
        assert!(ensure_single_export(&[keyed(snap.clone()), keyed(drain.clone())]).is_ok());

        let mut pg = manifest("r3", 10, None);
        pg.source.engine = "postgres".into();
        pg.source.table = Some("orders".into());
        pg.export_name = drain.export_name.clone();
        assert!(
            ensure_single_export(&[keyed(drain.clone()), keyed(pg)]).is_err(),
            "two engines under one name is still two sources"
        );
        let mut other_tbl = manifest("r4", 10, None);
        other_tbl.source.engine = "mysql".into();
        other_tbl.source.table = Some("customers".into());
        other_tbl.export_name = drain.export_name.clone();
        assert!(
            ensure_single_export(&[keyed(drain), keyed(other_tbl)]).is_err(),
            "two named tables under one name is still two sources"
        );
        let mut bare_my = manifest("r5", 10, None);
        bare_my.source.engine = "mysql".into();
        bare_my.source.table = None;
        let mut bare_pg = manifest("r6", 10, None);
        bare_pg.source.engine = "postgres".into();
        bare_pg.source.table = None;
        bare_pg.export_name = bare_my.export_name.clone();
        assert!(ensure_single_export(&[keyed(bare_my), keyed(bare_pg)]).is_err());
    }

    /// Regression: the CDC `initial: snapshot` leg shares the drain's prefix BY DESIGN (`cdc_job` synthesizes `{export}__snapshot_{table}` pointed at the same destination).
    #[test]
    fn ensure_single_export_admits_the_cdc_snapshot_leg_of_the_same_export() {
        let keyed = |m: RunManifest| ("gs://b/p/manifest-x.json".to_string(), m);
        let drain = keyed(manifest("r1", 10, None));
        let mut snap = manifest("r2", 10, None);
        snap.export_name = "orders__snapshot_orders".into();
        assert!(ensure_single_export(&[drain.clone(), keyed(snap)]).is_ok());
        let mut foreign = manifest("r3", 10, None);
        foreign.export_name = "customers__snapshot_customers".into();
        assert!(ensure_single_export(&[drain, keyed(foreign)]).is_err());
    }

    /// The documented `initial: snapshot` config.
    #[test]
    fn recorded_family_reconciles_the_documented_name_ne_table_config() {
        let keyed = |m: RunManifest| ("gs://b/p/manifest-x.json".to_string(), m);
        let mut drain = manifest("r1", 10, None);
        drain.export_name = "orders".into();
        drain.export_family = "orders_cdc".into();
        let mut snap = manifest("r2", 10, None);
        snap.export_name = "orders_cdc__snapshot_orders".into();
        snap.export_family = "orders_cdc".into();
        assert!(
            ensure_single_export(&[keyed(drain), keyed(snap)]).is_ok(),
            "one export whose name differs from its table must reconcile as ONE family"
        );
    }

    /// The inverse hole the substring fold had: an export the user literally NAMED with the infix.
    #[test]
    fn a_user_export_named_like_a_snapshot_leg_no_longer_disarms_the_guard() {
        let daily = crate::config::sample_export("daily");
        let mut tricky = crate::config::sample_export("daily__snapshot_v2");
        tricky.snapshot_parent = None;
        assert_eq!(daily.family(), "daily");
        assert_eq!(
            tricky.family(),
            "daily__snapshot_v2",
            "a user export's name is its own family — folding it merges two exports"
        );

        let keyed = |m: RunManifest| ("gs://b/p/manifest-x.json".to_string(), m);
        let mut a = manifest("r1", 10, None);
        a.export_name = daily.name.clone();
        a.export_family = daily.family();
        let mut b = manifest("r2", 10, None);
        b.export_name = tricky.name.clone();
        b.export_family = tricky.family();
        assert!(
            ensure_single_export(&[keyed(a), keyed(b)]).is_err(),
            "two distinct exports must refuse, even when one's NAME contains the infix"
        );
    }

    /// The synthesized leg is the ONE case where the infix name does mean a family.
    #[test]
    fn the_synthesized_snapshot_leg_takes_its_parents_family() {
        let export = crate::config::sample_export("orders_cdc");
        let leg =
            crate::pipeline::cdc_job::synth_snapshot_export_for_test(&export, "orders", "orders");
        assert_eq!(
            leg.snapshot_parent.as_deref(),
            Some("orders_cdc"),
            "the leg must RECORD its parent, not leave it to be re-derived"
        );
        assert!(
            leg.name.contains(crate::manifest::SNAPSHOT_LEG_INFIX),
            "sanity: the leg is the infix-named export"
        );
    }

    /// UPGRADE COMPAT.
    #[test]
    fn a_legacy_manifest_joins_the_recorded_family_of_the_same_export() {
        let keyed = |m: RunManifest| ("gs://b/p/manifest-x.json".to_string(), m);
        let mut legacy = manifest("r1", 10, None);
        legacy.export_name = "orders".into();
        legacy.export_family = String::new();
        let mut fresh = manifest("r2", 10, None);
        fresh.export_name = "orders".into();
        fresh.export_family = "orders_cdc".into();
        assert!(
            ensure_single_export(&[keyed(legacy), keyed(fresh)]).is_ok(),
            "a pre-upgrade manifest beside its post-upgrade successor is ONE export"
        );
    }

    /// The other half: tolerance must not blind the guard.
    #[test]
    fn a_legacy_manifest_of_another_export_is_still_refused() {
        let keyed = |m: RunManifest| ("gs://b/p/manifest-x.json".to_string(), m);
        let mut legacy = manifest("r1", 10, None);
        legacy.export_name = "customers".into();
        legacy.export_family = String::new();
        let mut fresh = manifest("r2", 10, None);
        fresh.export_name = "orders".into();
        fresh.export_family = "orders_cdc".into();
        assert!(
            ensure_single_export(&[keyed(legacy), keyed(fresh)]).is_err(),
            "a legacy manifest of a DIFFERENT export must still be refused"
        );
    }

    /// DOCUMENTS the contract a snapshot leg\'s `running` marker must satisfy.
    #[test]
    fn a_snapshot_legs_running_marker_documents_the_family_contract() {
        let keyed = |m: RunManifest| ("gs://b/p/manifest-x.json".to_string(), m);
        let mut drain = manifest("r1", 10, None);
        drain.export_name = "orders".into();
        drain.export_family = "sales".into();
        let mut marker = manifest("r2", 0, None);
        marker.export_name = "sales__snapshot_orders".into();
        marker.export_family = "sales".into();
        marker.status = ManifestStatus::Running;
        assert!(
            ensure_single_export(&[keyed(drain.clone()), keyed(marker.clone())]).is_ok(),
            "a leg\'s marker belongs to its parent\'s family"
        );
        let mut wrong = marker;
        wrong.export_family = "sales__snapshot_orders".into();
        assert!(
            ensure_single_export(&[keyed(drain), keyed(wrong)]).is_err(),
            "recording the decorated name reads as a second export — the bricked prefix"
        );
    }

    /// ONE aborted run must not brick the prefix.
    #[test]
    fn a_failed_manifest_does_not_block_the_successful_runs() {
        let keyed = |m: RunManifest| ("gs://b/p/manifest-x.json".to_string(), m);
        let mut failed = manifest("r7", 5, None);
        failed.status = ManifestStatus::Failed;
        let ok8 = manifest("r8", 10, None);
        let ok9 = manifest("r9", 10, None);
        let sel = select_runs(
            vec![keyed(failed), keyed(ok8), keyed(ok9)],
            &std::collections::HashSet::new(),
            crate::load::plan::LoadMode::Cdc,
        )
        .unwrap();
        let ids: Vec<&str> = sel.iter().map(|(_, m)| m.run_id.as_str()).collect();
        assert_eq!(
            ids,
            vec!["r8", "r9"],
            "the successful runs must still load; the aborted run is skipped, not fatal"
        );
        let manifests: Vec<RunManifest> = sel.into_iter().map(|(_, m)| m).collect();
        assert!(
            reconcile(&manifests, false).is_ok(),
            "reconcile must not see the aborted run at all"
        );
    }

    /// A run that captured NOTHING must not turn the selection into a full-prefix load.
    #[test]
    fn a_zero_part_manifest_does_not_drag_the_whole_prefix_into_the_load() {
        let mut empty = manifest("r_anchor", 0, None);
        empty.parts = Vec::new();
        empty.part_count = 0;
        empty.row_count = 0;
        let all = vec!["base/a.parquet".to_string(), "base/b.parquet".to_string()];
        let picked = select_load_keys(
            &[("gs://b/base/manifest-r_anchor.json".to_string(), empty)],
            &all,
        );
        assert!(
            picked.is_empty(),
            "a run with no parts contributes no files; it must not fall back to the \
             whole prefix (got {picked:?}), or the load appends rows the manifests \
             never accounted for"
        );
    }

    #[test]
    fn source_rows_is_none_when_no_manifest_probed_the_source() {
        let ms = vec![manifest("r1", 100, None), manifest("r2", 40, None)];
        let got = reconcile(&ms, false).unwrap();
        assert_eq!(got.file_rows, 140);
        assert_eq!(
            got.source_rows, None,
            "unprobed source is unknown, not zero"
        );
    }

    #[test]
    fn source_rows_present_even_if_only_some_manifests_probed() {
        let ms = vec![manifest("r1", 100, Some(100)), manifest("r2", 40, None)];
        let got = reconcile(&ms, false).unwrap();
        assert_eq!(got.source_rows, Some(100));
    }

    #[test]
    fn empty_manifests_refuses_to_load() {
        let err = reconcile(&[], false).unwrap_err().to_string();
        assert!(err.contains("refusing to load"), "{err}");
    }

    #[test]
    fn non_success_manifest_refuses_to_load() {
        let mut m = manifest("r1", 100, Some(100));
        m.status = ManifestStatus::Interrupted;
        let err = reconcile(&[m], false).unwrap_err().to_string();
        assert!(err.contains("not Success"), "{err}");
    }

    #[test]
    fn self_inconsistent_manifest_refuses_to_load() {
        let mut m = manifest("r1", 100, Some(100));
        m.row_count = 999;
        let err = reconcile(&[m], false).unwrap_err().to_string();
        assert!(err.contains("inconsistent"), "{err}");
    }

    #[test]
    fn source_file_mismatch_hard_fails_by_default() {
        let m = manifest("r1", 100, Some(120));
        let err = reconcile(&[m], false).unwrap_err().to_string();
        assert!(err.contains("source→file mismatch"), "{err}");
        assert!(err.contains("dropped 20"), "{err}");
    }

    /// Every refusal of the manifests and parts a load would consume names its code and class.
    #[test]
    fn each_reconcile_refusal_carries_its_code() {
        let code = |r: Result<LoadIntegrity>| {
            let e = r.unwrap_err();
            (
                crate::error::error_code(&e),
                crate::error::classify_exit(&e),
            )
        };
        let refused = (Some("RIVET_LOAD_RUN_NOT_LOADABLE"), 5);
        assert_eq!(code(reconcile(&[], false)), refused);
        let mut partial = manifest("r1", 100, Some(100));
        partial.status = ManifestStatus::Interrupted;
        assert_eq!(code(reconcile(&[partial], false)), refused);
        let mut lying = manifest("r1", 100, Some(100));
        lying.row_count = 999;
        assert_eq!(
            code(reconcile(&[lying], false)),
            (Some("RIVET_LOAD_MANIFEST_INCONSISTENT"), 3)
        );
        assert_eq!(
            code(reconcile(&[manifest("r1", 100, Some(120))], false)),
            (Some("RIVET_LOAD_SOURCE_FILE_MISMATCH"), 3)
        );

        let keyed = |m: RunManifest| ("gs://b/p/manifest-x.json".to_string(), m);
        let mut customers = manifest("r2", 10, None);
        customers.export_name = "customers".into();
        let mut mysql = manifest("r3", 10, None);
        mysql.source.engine = "mysql".into();
        for pair in [
            [keyed(manifest("r1", 10, None)), keyed(customers)],
            [keyed(manifest("r1", 10, None)), keyed(mysql)],
        ] {
            let e = ensure_single_export(&pair).unwrap_err();
            assert_eq!(
                crate::error::error_code(&e),
                Some("RIVET_LOAD_SOURCES_SHARED")
            );
        }

        let (store, _g) = fs_store(&[]);
        let gone = [keyed(manifest("r1", 10, None))];
        let e = select_load_uris(&store, "gs://my-bucket/base", &gone).unwrap_err();
        assert_eq!(
            crate::error::error_code(&e),
            Some("RIVET_LOAD_RUN_NOT_LOADABLE")
        );
        let big = vec![b' '; MANIFEST_MAX_BYTES as usize + 1];
        let (store, _g) = fs_store(&[("base/manifest-r1.json", big)]);
        let e = fetch_manifests_keyed(&store, "gs://my-bucket/base").unwrap_err();
        assert_eq!(
            crate::error::error_code(&e),
            Some("RIVET_LOAD_STAGING_OBJECT_UNSAFE")
        );
    }

    #[test]
    fn source_file_mismatch_is_allowed_under_the_override() {
        let m = manifest("r1", 100, Some(120));
        let got = reconcile(&[m], true).expect("allow_source_drift: true proceeds");
        assert_eq!(got.file_rows, 100);
        assert_eq!(
            got.source_rows,
            Some(120),
            "the probed source count is still surfaced"
        );
    }

    /// A `(manifest_key, manifest)` pair with a single part named `part`.
    fn keyed(key: &str, run: &str, part: &str) -> (String, RunManifest) {
        let mut m = manifest(run, 10, Some(10));
        m.parts[0].path = part.into();
        (key.to_string(), m)
    }

    #[test]
    fn select_load_keys_picks_only_the_new_runs_parts() {
        let all = vec![
            "base/r1-000.parquet".to_string(),
            "base/r2-000.parquet".to_string(),
        ];
        let new = vec![keyed("base/manifest-r2.json", "r2", "r2-000.parquet")];
        assert_eq!(
            select_load_keys(&new, &all),
            vec!["base/r2-000.parquet".to_string()],
            "loads r2's part only — not r1's already-loaded file"
        );
    }

    #[test]
    fn select_load_uris_lists_and_re_prefixes_selected_keys_with_the_source_bucket() {
        let (store, _g) = fs_store(&[
            ("base/r1-000.parquet", b"a".to_vec()),
            ("base/manifest-r1.json", b"{}".to_vec()),
        ]);
        let new = vec![keyed("base/manifest-r1.json", "r1", "r1-000.parquet")];
        assert_eq!(
            select_load_uris(&store, "gs://my-bucket/base", &new).unwrap(),
            vec!["gs://my-bucket/base/r1-000.parquet".to_string()],
            "the selected key, re-prefixed with the source bucket — never the manifest"
        );
    }

    #[test]
    fn select_load_keys_resolves_a_snapshot_subprefix_manifest() {
        let all = vec!["base/snapshot/snap-000.parquet".to_string()];
        let new = vec![keyed(
            "base/snapshot/manifest-r1.json",
            "r1",
            "snap-000.parquet",
        )];
        assert_eq!(
            select_load_keys(&new, &all),
            vec!["base/snapshot/snap-000.parquet".to_string()]
        );
    }

    #[test]
    fn select_load_keys_falls_back_to_full_listing_when_a_manifest_has_no_present_part() {
        let all = vec!["base/a.parquet".to_string(), "base/b.parquet".to_string()];
        let new = vec![keyed("base/manifest-r1.json", "r1", "missing.parquet")];
        assert_eq!(
            select_load_keys(&new, &all),
            all,
            "unresolvable part → blanket fallback"
        );
    }

    #[test]
    fn a_run_whose_declared_parts_are_all_gone_is_named_not_replaced_by_the_listing() {
        let all = vec!["base/older-run.parquet".to_string()];
        let gone = vec![keyed("base/manifest-r2.json", "r2", "r2-000.parquet")];
        assert_eq!(runs_with_missing_parts(&gone, &all), ["r2"]);
        let present = vec![keyed("base/manifest-r1.json", "r1", "older-run.parquet")];
        assert!(runs_with_missing_parts(&present, &all).is_empty());
    }

    #[test]
    fn select_load_keys_empty_new_set_selects_nothing_never_the_full_listing() {
        let all = vec![
            "base/r1-000.parquet".to_string(),
            "base/r2-000.parquet".to_string(),
        ];
        assert!(
            select_load_keys(&[], &all).is_empty(),
            "no new runs ⇒ load nothing, not everything"
        );
    }

    #[test]
    fn gc_orphans_removes_unmanifested_parquet_only() {
        let (store, _g) = fs_store(&[
            ("base/r1-000.parquet", b"aa".to_vec()),
            ("base/orphan.parquet", b"junk".to_vec()),
            ("base/manifest.json", b"{}".to_vec()),
            ("base/_SUCCESS", b"".to_vec()),
        ]);
        let keyed = vec![keyed("base/manifest-r1.json", "r1", "r1-000.parquet")];
        let (removed, bytes) =
            gc_orphans(&store, "gs://b/base", &keyed, false, &Default::default()).unwrap();
        assert_eq!(removed, 1, "only the unmanifested part is removed");
        assert_eq!(bytes, 4, "'junk' is 4 bytes");
        let mut left = store.list_files("base").unwrap();
        left.sort();
        assert_eq!(
            left,
            vec![
                "base/_SUCCESS".to_string(),
                "base/manifest.json".to_string(),
                "base/r1-000.parquet".to_string(),
            ],
            "the manifested part, the manifest, and _SUCCESS all survive"
        );
    }

    #[test]
    fn gc_orphans_of_an_all_manifested_prefix_removes_nothing() {
        let (store, _g) = fs_store(&[("base/r1-000.parquet", b"a".to_vec())]);
        let keyed = vec![keyed("base/manifest-r1.json", "r1", "r1-000.parquet")];
        assert_eq!(
            gc_orphans(&store, "gs://b/base", &keyed, false, &Default::default())
                .unwrap()
                .0,
            0
        );
    }

    #[test]
    fn gc_orphans_keeps_a_snapshot_subprefix_manifested_part() {
        let (store, _g) = fs_store(&[
            ("base/snapshot/s-000.parquet", b"a".to_vec()),
            ("base/orphan.parquet", b"x".to_vec()),
        ]);
        let keyed = vec![keyed("base/snapshot/manifest-s.json", "s", "s-000.parquet")];
        let (removed, _) =
            gc_orphans(&store, "gs://b/base", &keyed, false, &Default::default()).unwrap();
        assert_eq!(removed, 1, "the top-level orphan goes");
        assert_eq!(
            store.list_files("base/snapshot").unwrap(),
            vec!["base/snapshot/s-000.parquet".to_string()],
            "the snapshot-subprefix manifested part is kept"
        );
    }

    #[test]
    fn gc_orphans_deletes_a_superseded_terminal_runs_parts_even_while_a_run_is_active() {
        let (store, _g) = fs_store(&[("base/f-000.parquet", b"x".to_vec())]);
        let mut kv = keyed("base/manifest-f.json", "f", "f-000.parquet");
        kv.1.status = ManifestStatus::Failed;
        kv.1.started_at = "2026-01-01T00:00:00Z".into();
        let mut ok = manifest("f2", 10, Some(10));
        ok.started_at = "2026-01-02T00:00:00Z".into();
        let ok = ("base/manifest-f2.json".to_string(), ok);
        assert_eq!(
            gc_orphans(&store, "gs://b/base", &[kv, ok], true, &Default::default())
                .unwrap()
                .0,
            1
        );
    }

    /// ROUND-4 HIGH (keyset × gc): between two attempts of a checkpoint-resumable export there is NO live run.
    #[test]
    fn gc_orphans_spares_a_failed_runs_parts_until_a_success_supersedes_it() {
        let (store, _g) = fs_store(&[("base/page0.parquet", b"x".to_vec())]);
        let mut failed = keyed("base/manifest-r0.json", "r0", "page0.parquet");
        failed.1.status = ManifestStatus::Failed;
        failed.1.started_at = "2026-01-01T00:00:00Z".into();
        let (removed, _) =
            gc_orphans(&store, "gs://b/base", &[failed], false, &Default::default()).unwrap();
        assert_eq!(
            removed, 0,
            "a Failed run's committed pages are the next resume's payload, not debris"
        );
        assert!(
            store
                .list_files("base")
                .unwrap()
                .iter()
                .any(|k| k.ends_with("page0.parquet")),
            "the committed page must survive the between-attempts gc"
        );
    }

    #[test]
    fn gc_orphans_spares_a_terminal_runs_parts_that_an_active_resume_is_reusing() {
        let (store, _g) = fs_store(&[("base/chunk0.parquet", b"x".to_vec())]);
        let mut failed = keyed("base/manifest-r0.json", "r0", "chunk0.parquet");
        failed.1.status = ManifestStatus::Failed;
        failed.1.started_at = "2026-01-01T00:00:00Z".into();
        let live = running("r1", "2026-01-02T00:00:00Z");
        let (removed, _) = gc_orphans(
            &store,
            "gs://b/base",
            &[failed, live],
            true,
            &Default::default(),
        )
        .unwrap();
        assert_eq!(
            removed, 0,
            "a terminal run's parts that an active same-export resume is reusing must be spared"
        );
        assert!(
            store
                .list_files("base")
                .unwrap()
                .iter()
                .any(|k| k.ends_with("chunk0.parquet")),
            "the reused chunk part must survive gc while the resume is live"
        );
    }

    #[test]
    fn gc_orphans_spares_an_unmanifested_part_while_a_run_is_active() {
        let (store, _g) = fs_store(&[
            ("base/r1-000.parquet", b"aa".to_vec()),
            ("base/inflight.parquet", b"x".to_vec()),
        ]);
        let keyed = vec![keyed("base/manifest-r1.json", "r1", "r1-000.parquet")];
        let (removed, _) =
            gc_orphans(&store, "gs://b/base", &keyed, true, &Default::default()).unwrap();
        assert_eq!(
            removed, 0,
            "an unmanifested part is spared while a run is active"
        );
        assert!(
            store
                .list_files("base")
                .unwrap()
                .iter()
                .any(|k| k.ends_with("inflight.parquet")),
            "the live run's in-flight part must survive gc_orphans"
        );
    }

    #[test]
    fn gc_orphans_collects_an_unmanifested_part_when_no_run_is_active() {
        let (store, _g) = fs_store(&[
            ("base/r1-000.parquet", b"aa".to_vec()),
            ("base/dead-orphan.parquet", b"x".to_vec()),
        ]);
        let keyed = vec![keyed("base/manifest-r1.json", "r1", "r1-000.parquet")];
        assert_eq!(
            gc_orphans(&store, "gs://b/base", &keyed, false, &Default::default())
                .unwrap()
                .0,
            1,
            "with no active run, an unmanifested orphan is collected"
        );
    }

    /// A `running` MARKER manifest (schema-less, no parts) started at `started_at`.
    fn running(run: &str, started_at: &str) -> (String, RunManifest) {
        let mut m = manifest(run, 0, None);
        m.status = ManifestStatus::Running;
        m.started_at = started_at.into();
        m.finished_at = String::new();
        m.parts.clear();
        (format!("base/manifest-{run}.json"), m)
    }

    #[test]
    fn has_active_running_manifest_true_for_a_lone_running_marker() {
        assert!(has_active_running_manifest(&[running(
            "r1",
            "2026-01-01T00:00:00Z"
        )]));
    }

    #[test]
    fn a_later_started_split_sibling_finishing_first_does_not_mask_a_live_sibling() {
        let unit = |name: &str, status, started: &str| {
            let mut m = manifest(name, 0, None);
            m.export_name = name.into();
            m.export_family = "orders".into();
            m.status = status;
            m.started_at = started.into();
            m.finished_at = String::new();
            m.parts.clear();
            (format!("base/manifest-{name}.json"), m)
        };
        let keyed = vec![
            unit("orders#0", ManifestStatus::Running, "2026-01-01T00:00:00Z"),
            unit("orders#1", ManifestStatus::Running, "2026-01-01T00:00:01Z"),
            unit("orders#2", ManifestStatus::Success, "2026-01-01T00:00:02Z"),
        ];
        assert!(
            has_active_running_manifest(&keyed),
            "two still-Running split siblings must count as active even when a later-started \
             sibling has already finished — else a cross-host gc deletes their in-flight parts"
        );
        let crashed = unit("orders#0", ManifestStatus::Running, "2026-01-01T00:00:00Z");
        let successor = unit("orders#0", ManifestStatus::Running, "2026-01-02T00:00:00Z");
        let rerun = [crashed.clone(), successor];
        let census = ManifestCensus::new(&rerun);
        assert!(
            !census.superseded(&census.runs()[0]),
            "a running successor proves nothing about the older marker"
        );
        assert!(census.active_running());
        let done = unit("orders#0", ManifestStatus::Success, "2026-01-02T00:00:00Z");
        let rerun = [crashed, done];
        let census = ManifestCensus::new(&rerun);
        assert!(
            census.superseded(&census.runs()[0]),
            "a SUCCESS re-run of the SAME unit (orders#0) supersedes its crashed marker"
        );
    }

    #[test]
    fn has_active_running_manifest_false_when_none_is_running() {
        assert!(!has_active_running_manifest(&[keyed(
            "k",
            "r1",
            "p.parquet"
        )]));
    }

    #[test]
    fn has_active_running_manifest_false_for_a_superseded_running_marker() {
        let r1 = running("r1", "2026-01-01T00:00:00Z");
        let mut r2 = manifest("r2", 10, Some(10));
        r2.started_at = "2026-01-02T00:00:00Z".into();
        assert!(!has_active_running_manifest(&[
            r1,
            ("base/manifest-r2.json".into(), r2)
        ]));
    }

    #[test]
    fn select_runs_drops_a_running_manifest() {
        let run_marker = running("r_running", "2026-01-02T00:00:00Z");
        let ok = keyed("base/manifest-r_ok.json", "r_ok", "r_ok-000.parquet");
        let sel = select_runs(
            vec![run_marker, ok],
            &std::collections::HashSet::new(),
            crate::load::plan::LoadMode::Incremental,
        )
        .unwrap();
        assert_eq!(sel.len(), 1, "only the Success run is selected");
        assert_eq!(sel[0].1.run_id, "r_ok");
        assert!(sel.iter().all(|(_, m)| m.status != ManifestStatus::Running));
    }

    #[test]
    fn gc_orphans_removes_a_superseded_running_marker_but_spares_a_live_one() {
        let r1 = running("r1", "2026-01-01T00:00:01Z");
        let mut ok = manifest("r2", 10, Some(10));
        ok.started_at = "2026-01-01T00:00:02Z".into();
        let r2 = ("base/manifest-r2.json".to_string(), ok);
        let (store, _g) = fs_store(&[planted(&r1), planted(&r2)]);
        let (removed, _) =
            gc_orphans(&store, "gs://b/base", &[r1, r2], true, &Default::default()).unwrap();
        assert_eq!(removed, 1, "only the superseded running marker is removed");
        let left = store.list_files("base").unwrap();
        assert!(
            !left.iter().any(|k| k.ends_with("manifest-r1.json")),
            "the superseded (dead) running marker is deleted"
        );
        assert!(
            left.iter().any(|k| k.ends_with("manifest-r2.json")),
            "the successor's manifest survives"
        );

        let (store, _g) = fs_store(&[
            ("base/manifest-r1.json", b"{}".to_vec()),
            ("base/manifest-r2.json", b"{}".to_vec()),
        ]);
        let r1 = running("r1", "2026-01-01T00:00:01Z");
        let r2 = running("r2", "2026-01-01T00:00:02Z");
        let (removed, _) =
            gc_orphans(&store, "gs://b/base", &[r1, r2], true, &Default::default()).unwrap();
        assert_eq!(
            removed, 0,
            "an overlapping running successor sweeps NOTHING"
        );
        assert_eq!(store.list_files("base").unwrap().len(), 2);
    }

    /// ROUND-5 (three agents): a `running` marker whose run_id the LEDGER says is TERMINAL is a dead crash marker even though no Success ever superseded it.

    #[test]
    fn the_sweep_retires_a_running_marker_whose_ledger_row_is_terminal() {
        let r1 = running("r1", "2026-01-01T00:00:01Z");
        let r2 = running("r2", "2026-01-01T00:00:02Z");
        let (store, _g) = fs_store(&[planted(&r1), planted(&r2)]);
        let dead = std::collections::HashSet::from(["r1".to_string()]);
        let (removed, _) = gc_orphans(&store, "gs://b/base", &[r1, r2], true, &dead).unwrap();
        assert_eq!(removed, 1, "only the ledger-terminal marker is swept");
        let left = store.list_files("base").unwrap();
        assert!(!left.iter().any(|k| k.ends_with("manifest-r1.json")));
        assert!(
            left.iter().any(|k| k.ends_with("manifest-r2.json")),
            "a marker the ledger still calls running survives"
        );
    }

    /// A marker that FINISHED between the census and the sweep is spared.
    #[test]
    fn the_sweep_spares_a_marker_that_finished_after_the_census_was_taken() {
        let stale = running("r1", "2026-01-01T00:00:01Z");
        let mut done = manifest("r1", 10, Some(10));
        done.started_at = "2026-01-01T00:00:01Z".into();
        let (store, _g) =
            fs_store(&[("base/manifest-r1.json", serde_json::to_vec(&done).unwrap())]);

        let dead = std::collections::HashSet::from(["r1".to_string()]);
        let (removed, _) = gc_orphans(&store, "gs://b/base", &[stale], true, &dead).unwrap();

        assert_eq!(
            removed, 0,
            "the ledger says terminal and the SNAPSHOT says running — but the object is \
             now a Success manifest, and deleting it orphans every part it declares"
        );
        assert!(
            store
                .list_files("base")
                .unwrap()
                .iter()
                .any(|k| k.ends_with("manifest-r1.json")),
            "the terminal manifest survives"
        );
    }

    fn keyed_at(run: &str, finished_at: &str) -> (String, RunManifest) {
        let mut m = manifest(run, 100, None);
        m.finished_at = finished_at.into();
        (format!("base/manifest-{run}.json"), m)
    }

    #[test]
    fn latest_full_picks_the_newest_snapshot_not_all() {
        let keyed = vec![
            keyed_at("r1", "2026-01-01T00:00:00Z"),
            keyed_at("r3", "2026-01-03T00:00:00Z"),
            keyed_at("r2", "2026-01-02T00:00:00Z"),
        ];
        let sel = latest_full(keyed);
        assert_eq!(sel.len(), 1, "exactly one snapshot, never all");
        assert_eq!(sel[0].1.run_id, "r3", "the newest by finished_at");
    }

    /// Two unloaded baseline generations must not both overwrite the base (every key twice).
    #[test]
    fn a_base_load_writes_only_the_newest_baseline_generation() {
        let (sel, superseded) = latest_baseline_generation(vec![
            keyed_at("b1", "2026-01-01T00:00:00Z"),
            keyed_at("b2", "2026-01-02T00:00:00Z"),
        ])
        .unwrap();
        let picked: Vec<&str> = sel.iter().map(|(_, m)| m.run_id.as_str()).collect();
        assert_eq!(picked, ["b2"]);
        assert_eq!(superseded, ["b1"]);

        let unit = |name: &str, run: &str| {
            let mut m = manifest(run, 100, None);
            m.export_name = name.into();
            m.export_family = "orders".into();
            (format!("orders/manifest-{run}.json"), m)
        };
        let (sel, superseded) =
            latest_baseline_generation(vec![unit("orders#0", "u0"), unit("orders#1", "u1")])
                .unwrap();
        assert_eq!(sel.len(), 2, "every split unit of one generation loads");
        assert!(superseded.is_empty());
    }

    #[test]
    fn latest_full_over_a_split_family_selects_every_unit_not_just_the_last() {
        let unit = |name: &str, run: &str, at: &str| {
            let mut m = manifest(run, 100, None);
            m.export_name = name.into();
            m.export_family = "orders".into();
            m.finished_at = at.into();
            (format!("orders/manifest-{run}.json"), m)
        };
        let keyed = vec![
            unit("orders#0", "r0", "2026-01-01T00:00:01Z"),
            unit("orders#1", "r1", "2026-01-01T00:00:02Z"),
            unit("orders#2", "r2", "2026-01-01T00:00:03Z"),
        ];
        let mut sel = latest_full(keyed);
        sel.sort_by(|a, b| a.1.export_name.cmp(&b.1.export_name));
        let names: Vec<&str> = sel.iter().map(|(_, m)| m.export_name.as_str()).collect();
        assert_eq!(
            names,
            vec!["orders#0", "orders#1", "orders#2"],
            "a Full load over a split prefix must select EVERY unit — one per {{family}}#i — \
             not just the unit that finished last"
        );
    }

    #[test]
    fn latest_full_over_a_split_family_takes_the_newest_run_per_unit() {
        let unit = |name: &str, run: &str, at: &str| {
            let mut m = manifest(run, 100, None);
            m.export_name = name.into();
            m.export_family = "orders".into();
            m.finished_at = at.into();
            (format!("orders/manifest-{run}.json"), m)
        };
        let keyed = vec![
            unit("orders#0", "r0a", "2026-01-01T00:00:00Z"),
            unit("orders#0", "r0b", "2026-01-02T00:00:00Z"),
            unit("orders#1", "r1a", "2026-01-01T00:00:00Z"),
            unit("orders#1", "r1b", "2026-01-02T00:00:00Z"),
        ];
        let mut sel = latest_full(keyed);
        sel.sort_by(|a, b| a.1.export_name.cmp(&b.1.export_name));
        let picked: Vec<(&str, &str)> = sel
            .iter()
            .map(|(_, m)| (m.export_name.as_str(), m.run_id.as_str()))
            .collect();
        assert_eq!(
            picked,
            vec![("orders#0", "r0b"), ("orders#1", "r1b")],
            "each unit contributes its NEWEST run; no unit is dropped and no stale run loaded"
        );
    }

    #[test]
    fn select_runs_full_refuses_a_mixed_generation_split_prefix() {
        let unit = |name: &str, run: &str, lo: Option<&str>, hi: Option<&str>, at: &str| {
            let mut m = manifest(run, 100, None);
            m.export_name = name.into();
            m.export_family = "orders".into();
            m.finished_at = at.into();
            m.split_window = Some(crate::manifest::SplitWindow {
                key_column: "id".into(),
                lo: lo.map(String::from),
                hi: hi.map(String::from),
            });
            (format!("orders/manifest-{run}.json"), m)
        };
        let keyed = vec![
            unit("orders#0", "b0", None, Some("500"), "2026-01-02T00:00:00Z"),
            unit("orders#1", "b1", Some("500"), None, "2026-01-02T00:00:01Z"),
            unit("orders#2", "a2", Some("700"), None, "2026-01-01T00:00:00Z"),
        ];
        let err = select_runs(
            keyed,
            &std::collections::HashSet::new(),
            crate::load::plan::LoadMode::Full,
        )
        .expect_err("a mixed-generation split prefix must be refused, not silently loaded");
        assert!(
            err.to_string().contains("more than one split generation")
                || err.to_string().contains("DUPLICATE"),
            "error must name the multi-generation duplication hazard: {err}"
        );
    }

    #[test]
    fn select_runs_full_refuses_an_equal_count_split_generation_with_an_interior_hole() {
        let unit = |name: &str, run: &str, lo: Option<&str>, hi: Option<&str>, at: &str| {
            let mut m = manifest(run, 100, None);
            m.export_name = name.into();
            m.export_family = "orders".into();
            m.finished_at = at.into();
            m.split_window = Some(crate::manifest::SplitWindow {
                key_column: "id".into(),
                lo: lo.map(String::from),
                hi: hi.map(String::from),
            });
            (format!("orders/manifest-{run}.json"), m)
        };
        let keyed = vec![
            unit("orders#0", "b0", None, Some("200"), "2026-01-02T00:00:00Z"),
            unit(
                "orders#1",
                "b1",
                Some("200"),
                Some("400"),
                "2026-01-02T00:00:01Z",
            ),
            unit(
                "orders#2",
                "a2",
                Some("500"),
                Some("750"),
                "2026-01-01T00:00:00Z",
            ),
            unit("orders#3", "b3", Some("600"), None, "2026-01-02T00:00:03Z"),
        ];
        let err = select_runs(
            keyed,
            &std::collections::HashSet::new(),
            crate::load::plan::LoadMode::Full,
        )
        .expect_err(
            "an equal-count split generation with a gap+overlap must be refused, not loaded",
        );
        assert!(
            err.to_string().contains("tile") || err.to_string().contains("INCOHERENT"),
            "error must name the non-tiling (gap+overlap) hazard: {err}"
        );
    }

    #[test]
    fn select_runs_full_accepts_one_coherent_split_generation() {
        let unit = |name: &str, run: &str, lo: Option<&str>, hi: Option<&str>| {
            let mut m = manifest(run, 100, None);
            m.export_name = name.into();
            m.export_family = "orders".into();
            m.split_window = Some(crate::manifest::SplitWindow {
                key_column: "id".into(),
                lo: lo.map(String::from),
                hi: hi.map(String::from),
            });
            (format!("orders/manifest-{run}.json"), m)
        };
        let keyed = vec![
            unit("orders#0", "r0", None, Some("500")),
            unit("orders#1", "r1", Some("500"), None),
        ];
        let sel = select_runs(
            keyed,
            &std::collections::HashSet::new(),
            crate::load::plan::LoadMode::Full,
        )
        .unwrap();
        assert_eq!(
            sel.len(),
            2,
            "both units of one coherent generation must load"
        );
    }

    #[test]
    fn latest_full_re_materializes_even_when_the_latest_is_already_loaded() {
        let keyed = vec![
            keyed_at("r1", "2026-01-01T00:00:00Z"),
            keyed_at("r2", "2026-01-02T00:00:00Z"),
        ];
        let sel = latest_full(keyed);
        assert_eq!(sel.len(), 1);
        assert_eq!(sel[0].1.run_id, "r2", "always the latest, loaded or not");
    }

    #[test]
    fn latest_full_of_no_staged_runs_is_empty_so_the_caller_no_ops_without_truncating() {
        assert!(latest_full(Vec::new()).is_empty());
    }

    #[test]
    fn latest_full_orders_by_parsed_instant_not_lexical_bytes() {
        let keyed = vec![
            keyed_at("older", "2026-01-01T00:00:00Z"),
            keyed_at("newer", "2026-01-01T00:00:00.500Z"),
        ];
        let sel = latest_full(keyed);
        assert_eq!(sel.len(), 1);
        assert_eq!(
            sel[0].1.run_id, "newer",
            "the fractional-second run is the newer instant"
        );
    }

    #[test]
    fn select_runs_full_picks_the_latest_even_when_loaded_and_even_when_stateless() {
        use crate::load::plan::LoadMode;
        use std::collections::HashSet;
        let keyed = vec![
            keyed_at("r1", "2026-01-01T00:00:00Z"),
            keyed_at("r2", "2026-01-02T00:00:00Z"),
        ];
        let loaded = HashSet::from(["r2".to_string()]);
        let sel = select_runs(keyed.clone(), &loaded, LoadMode::Full).unwrap();
        assert_eq!(sel.len(), 1);
        assert_eq!(sel[0].1.run_id, "r2", "Full picks latest, loaded or not");
        let sel = select_runs(keyed, &HashSet::new(), LoadMode::Full).unwrap();
        assert_eq!(sel.len(), 1, "stateless Full is not a blanket load");
        assert_eq!(sel[0].1.run_id, "r2");
    }

    /// A failed ledger read stops an APPEND load and is harmless to a full one.
    #[test]
    fn a_failed_ledger_read_stops_an_append_but_not_an_overwrite() {
        use crate::load::plan::LoadMode;
        assert!(
            ledger_read_failure_is_fatal(LoadMode::Incremental),
            "incremental selects what is NOT in the set — an empty one re-appends history"
        );
        assert!(
            ledger_read_failure_is_fatal(LoadMode::Cdc),
            "cdc selects what is NOT in the set — an empty one re-appends history"
        );
        assert!(
            !ledger_read_failure_is_fatal(LoadMode::Full),
            "full never consults the set: it takes the latest run and overwrites"
        );
    }

    #[test]
    fn select_runs_append_modes_filter_loaded_and_load_all_when_stateless() {
        use crate::load::plan::LoadMode;
        use std::collections::HashSet;
        let keyed = vec![
            keyed_at("r1", "2026-01-01T00:00:00Z"),
            keyed_at("r2", "2026-01-02T00:00:00Z"),
        ];
        let loaded = HashSet::from(["r1".to_string()]);
        for mode in [LoadMode::Incremental, LoadMode::Cdc] {
            let sel = select_runs(keyed.clone(), &loaded, mode).unwrap();
            assert_eq!(sel.len(), 1, "{mode:?}: only the unloaded run");
            assert_eq!(sel[0].1.run_id, "r2");
            assert_eq!(
                select_runs(keyed.clone(), &HashSet::new(), mode)
                    .unwrap()
                    .len(),
                2,
                "{mode:?}: stateless loads every run"
            );
        }
    }

    #[test]
    fn is_run_unique_manifest_needs_both_prefix_and_json() {
        assert!(is_run_unique_manifest("manifest-20260101T000000.json"));
        assert!(!is_run_unique_manifest("manifest.json"));
        assert!(!is_run_unique_manifest("manifest-abc.txt"));
        assert!(!is_run_unique_manifest("data.json"));
    }

    #[test]
    fn chain_prefix_renders_source_and_files() {
        let known = LoadIntegrity {
            source_rows: Some(100),
            file_rows: 100,
            manifests: 1,
        };
        assert_eq!(known.chain_prefix(), "source 100 → files 100");
        let unknown = LoadIntegrity {
            source_rows: None,
            file_rows: 40,
            manifests: 1,
        };
        assert_eq!(unknown.chain_prefix(), "source ? → files 40");
    }

    #[test]
    fn is_manifest_key_matches_only_the_final_segment() {
        assert!(is_manifest_key("gs://b/p/manifest.json"));
        assert!(is_manifest_key("manifest.json"));
        assert!(!is_manifest_key("gs://b/p/part-0.parquet"));
        assert!(!is_manifest_key("gs://b/p/x_manifest.json"));
    }

    /// Build an fs-backed [`GcsStore`] over a fresh tempdir seeded with `(bucket-relative-key, bytes)` objects.
    fn shaped(key: &str, run: &str, name: &str, family: &str, mode: &str) -> (String, RunManifest) {
        let mut m = manifest(run, 1, None);
        m.export_name = name.into();
        m.export_family = family.into();
        m.mode = mode.into();
        (key.to_string(), m)
    }

    /// The `init` layout: a batch export at `exports/t/` and a CDC export of the same name nested at `exports/t/cdc/t/`, with its baseline leg under `snapshot/`.
    fn nested_layout() -> Vec<(String, RunManifest)> {
        vec![
            shaped("exports/t/manifest-b1.json", "b1", "t", "t", "batch"),
            shaped(
                "exports/t/cdc/t/manifest-c1.json",
                "c1",
                "dbo.t",
                "t",
                "cdc",
            ),
            shaped(
                "exports/t/cdc/t/snapshot/manifest-s1.json",
                "s1",
                "t__snapshot_dbo_t",
                "t",
                "batch",
            ),
        ]
    }

    fn run_ids(keyed: &[(String, RunManifest)]) -> Vec<&str> {
        keyed.iter().map(|(_, m)| m.run_id.as_str()).collect()
    }

    #[test]
    fn a_batch_load_skips_the_runs_of_a_cdc_export_nested_in_its_prefix_and_says_so() {
        for mode in [
            crate::load::plan::LoadMode::Full,
            crate::load::plan::LoadMode::Incremental,
        ] {
            let (own, note) =
                own_manifests(nested_layout(), "gs://b/exports/t/", "t", mode).unwrap();
            assert_eq!(run_ids(&own), vec!["b1"], "{mode:?}");
            let note = note.expect("the skipped runs are named");
            assert!(
                note.contains("exports/t/cdc/t/, exports/t/cdc/t/snapshot/"),
                "{note}"
            );
        }
    }

    #[test]
    fn a_cdc_load_keeps_its_stream_and_its_baseline_leg() {
        let (own, note) = own_manifests(
            nested_layout()[1..].to_vec(),
            "gs://b/exports/t/cdc/t/",
            "t",
            crate::load::plan::LoadMode::Cdc,
        )
        .unwrap();
        assert_eq!(run_ids(&own), vec!["c1", "s1"]);
        assert!(note.is_none(), "{note:?}");
    }

    /// A batch export switched to CDC (or back) writes both shapes into ONE directory.
    #[test]
    fn a_cdc_run_at_the_prefix_or_beside_a_batch_run_is_the_same_export() {
        for keyed in [
            vec![shaped(
                "exports/t/manifest-c1.json",
                "c1",
                "dbo.t",
                "t",
                "cdc",
            )],
            vec![
                shaped("exports/t/p=1/manifest-b1.json", "b1", "t", "t", "batch"),
                shaped("exports/t/p=1/manifest-c1.json", "c1", "dbo.t", "t", "cdc"),
            ],
        ] {
            let n = keyed.len();
            let (own, note) = own_manifests(
                keyed,
                "gs://b/exports/t",
                "t",
                crate::load::plan::LoadMode::Full,
            )
            .unwrap();
            assert_eq!(own.len(), n);
            assert!(note.is_none(), "{note:?}");
        }
    }

    /// Another export's runs are not skipped: the shared-prefix guard refuses them.
    #[test]
    fn a_nested_run_of_another_export_is_left_for_the_shared_prefix_guard() {
        let keyed = vec![
            shaped("exports/t/manifest-b1.json", "b1", "t", "t", "batch"),
            shaped("exports/t/u/manifest-c1.json", "c1", "dbo.u", "u", "cdc"),
        ];
        let (own, note) = own_manifests(
            keyed,
            "gs://b/exports/t/",
            "t",
            crate::load::plan::LoadMode::Full,
        )
        .unwrap();
        assert_eq!(run_ids(&own), vec!["b1", "c1"]);
        assert!(note.is_none());
        let err = ensure_single_export(&own).unwrap_err();
        assert_eq!(
            crate::error::error_code(&err),
            Some("RIVET_LOAD_SOURCES_SHARED")
        );
    }

    fn fs_store(files: &[(&str, Vec<u8>)]) -> (GcsStore, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        for (rel, bytes) in files {
            let p = dir.path().join(rel);
            std::fs::create_dir_all(p.parent().unwrap()).unwrap();
            std::fs::write(p, bytes).unwrap();
        }
        let store = GcsStore::open_fs(dir.path().to_str().unwrap()).unwrap();
        (store, dir)
    }

    /// The `(key, body)` pair for a census entry — the object the product would really have written at that key.
    fn planted(entry: &(String, RunManifest)) -> (&str, Vec<u8>) {
        (entry.0.as_str(), serde_json::to_vec(&entry.1).unwrap())
    }

    /// A run keyed at `key` whose parts are `parts`.
    fn keyed_run(id: &str, key: &str, parts: &[&str]) -> (String, RunManifest) {
        let mut m = manifest(id, 1, None);
        m.parts = parts
            .iter()
            .map(|p| ManifestPart {
                path: (*p).to_string(),
                ..m.parts[0].clone()
            })
            .collect();
        (key.to_string(), m)
    }

    #[test]
    fn cleanup_deletes_only_the_files_of_runs_nobody_is_writing() {
        let runs = [
            keyed_run("r1", "base/manifest-r1.json", &["a.parquet"]),
            keyed_run("r2", "base/manifest-r2.json", &["b.parquet"]),
        ];
        let writing: std::collections::HashSet<String> = ["r2".to_string()].into();
        let cleanup = crate::load::staging::cleanup_keys(&runs, &writing, |_| Some("r1".into()));
        assert_eq!(
            cleanup.keys,
            vec![
                "base/a.parquet".to_string(),
                "base/manifest-r1.json".to_string(),
            ],
            "r1's manifest and part go; r2 is still writing"
        );
        assert_eq!(
            cleanup.canonical_dirs(),
            ["base".to_string()],
            "the canonical pointer to r1 is retired with it"
        );
    }

    #[test]
    fn a_canonical_manifest_describing_another_run_stays() {
        let runs = [keyed_run("r1", "base/manifest-r1.json", &["a.parquet"])];
        let cleanup =
            crate::load::staging::cleanup_keys(&runs, &Default::default(), |_| Some("r9".into()));
        assert!(
            cleanup.canonical_dirs().is_empty(),
            "no canonical pointer is retired"
        );
        assert_eq!(
            cleanup.keys,
            vec![
                "base/a.parquet".to_string(),
                "base/manifest-r1.json".to_string()
            ],
            "a canonical manifest written by another run is not this load's to delete"
        );
    }

    fn manifest_bytes(run: &str, rows: i64, source: Option<i64>) -> Vec<u8> {
        serde_json::to_vec(&manifest(run, rows, source)).unwrap()
    }

    #[test]
    fn dedupe_drops_the_pointer_but_keeps_a_legacy_canonical_only_run() {
        let keyed = vec![
            (
                "base/manifest.json".to_string(),
                serde_json::from_slice::<RunManifest>(&manifest_bytes("r2", 40, Some(40))).unwrap(),
            ),
            (
                "base/manifest-r1.json".to_string(),
                serde_json::from_slice::<RunManifest>(&manifest_bytes("r1", 100, Some(100)))
                    .unwrap(),
            ),
            (
                "base/manifest-r2.json".to_string(),
                serde_json::from_slice::<RunManifest>(&manifest_bytes("r2", 40, Some(40))).unwrap(),
            ),
        ];
        let mut ids: Vec<String> = dedupe_by_run_id(keyed)
            .into_iter()
            .map(|(_, m)| m.run_id)
            .collect();
        ids.sort();
        assert_eq!(ids, vec!["r1".to_string(), "r2".to_string()]);

        let keyed = vec![
            (
                "base/manifest.json".to_string(),
                serde_json::from_slice::<RunManifest>(&manifest_bytes("r0", 7, Some(7))).unwrap(),
            ),
            (
                "base/manifest-r1.json".to_string(),
                serde_json::from_slice::<RunManifest>(&manifest_bytes("r1", 100, Some(100)))
                    .unwrap(),
            ),
        ];
        let mut ids: Vec<String> = dedupe_by_run_id(keyed)
            .into_iter()
            .map(|(_, m)| m.run_id)
            .collect();
        ids.sort();
        assert_eq!(
            ids,
            vec!["r0".to_string(), "r1".to_string()],
            "a legacy canonical-only run must not be silently dropped"
        );
    }

    #[test]
    fn list_manifest_keys_falls_back_to_the_canonical_name_for_a_single_run() {
        let (store, _g) = fs_store(&[("base/manifest.json", b"{}".to_vec())]);
        assert_eq!(
            list_manifest_keys(&store, "base").unwrap(),
            vec!["base/manifest.json".to_string()]
        );
    }

    #[test]
    fn fetch_manifests_keyed_refuses_a_manifest_over_the_cap() {
        let big = vec![b' '; MANIFEST_MAX_BYTES as usize + 1];
        let (store, _g) = fs_store(&[("base/manifest-r1.json", big)]);
        let err = fetch_manifests_keyed(&store, "gs://my-bucket/base").unwrap_err();
        assert!(format!("{err:#}").contains("CWE-400"), "{err:#}");
    }

    #[test]
    fn fetch_manifests_keyed_reads_and_parses_every_run_copy_under_the_prefix() {
        let (store, _g) = fs_store(&[
            ("base/manifest.json", manifest_bytes("r2", 40, Some(40))),
            (
                "base/manifest-r1.json",
                manifest_bytes("r1", 100, Some(100)),
            ),
            ("base/manifest-r2.json", manifest_bytes("r2", 40, Some(40))),
        ]);
        let manifests: Vec<_> = fetch_manifests_keyed(&store, "gs://my-bucket/base")
            .unwrap()
            .into_iter()
            .map(|(_, m)| m)
            .collect();
        assert_eq!(manifests.len(), 2);
        let integrity = reconcile(&manifests, false).unwrap();
        assert_eq!(integrity.file_rows, 140);
        assert_eq!(integrity.manifests, 2);
    }

    #[test]
    fn fetch_manifests_keyed_names_the_key_when_a_manifest_is_unparseable() {
        let (store, _g) = fs_store(&[("base/manifest.json", b"{ not json".to_vec())]);
        let err = fetch_manifests_keyed(&store, "gs://my-bucket/base")
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("parsing manifest") && err.contains("base/manifest.json"),
            "error should name the offending key: {err}"
        );
    }

    const FAKE_GCS_ENDPOINT: &str = "http://127.0.0.1:4443";
    const FAKE_GCS_BUCKET: &str = "rivet-load-emulator";

    /// A real GCS-transport [`GcsStore`] against the local emulator, scoped to `FAKE_GCS_BUCKET`.
    fn fake_gcs_store() -> GcsStore {
        let created = std::process::Command::new("curl")
            .args([
                "-s",
                "-X",
                "POST",
                &format!("{FAKE_GCS_ENDPOINT}/storage/v1/b?project=rivet-test"),
                "-H",
                "Content-Type: application/json",
                "-d",
                &format!("{{\"name\":\"{FAKE_GCS_BUCKET}\"}}"),
            ])
            .output();
        assert!(
            created.is_ok_and(|o| o.status.success()),
            "could not reach fake-gcs to create the bucket — is `docker compose up -d fake-gcs` running on :4443?"
        );
        let cfg = crate::config::DestinationConfig {
            destination_type: crate::config::DestinationType::Gcs,
            bucket: Some(FAKE_GCS_BUCKET.into()),
            endpoint: Some(FAKE_GCS_ENDPOINT.into()),
            allow_anonymous: true,
            ..Default::default()
        };
        GcsStore::new(&cfg).expect("build GcsStore against fake-gcs")
    }

    /// Per-object drain of `prefix` — list + single `remove` each.
    fn drain(store: &GcsStore, prefix: &str) {
        for key in store.list_files(prefix).unwrap() {
            store.remove(&key).unwrap();
        }
    }

    #[test]
    #[ignore = "emulator: needs `docker compose up -d fake-gcs` (fsouza/fake-gcs-server :4443)"]
    fn storage_contract_over_fake_gcs() {
        let store = fake_gcs_store();
        let prefix = "load-contract/orders";
        drain(&store, prefix);
        let gs = format!("gs://{FAKE_GCS_BUCKET}/{prefix}");

        store
            .put(
                &format!("{prefix}/manifest-r1.json"),
                &manifest_bytes("r1", 100, Some(100)),
            )
            .unwrap();
        store
            .put(&format!("{prefix}/part-000000.parquet"), b"rows-of-r1")
            .unwrap();
        store
            .put(&format!("{prefix}/orphan.parquet"), b"crash-leftover")
            .unwrap();

        let keyed = fetch_manifests_keyed(&store, &gs).unwrap();
        assert_eq!(keyed.len(), 1, "the run's manifest, read back over GCS");

        let manifests: Vec<_> = keyed.iter().map(|(_, m)| m.clone()).collect();
        assert_eq!(
            reconcile(&manifests, false).unwrap().file_rows,
            100,
            "file_rows drives the count-gate; a bad GCS read would corrupt it"
        );

        assert_eq!(
            select_load_uris(&store, &gs, &keyed).unwrap(),
            vec![format!(
                "gs://{FAKE_GCS_BUCKET}/{prefix}/part-000000.parquet"
            )],
            "load pulls the manifested part, not the unmanifested crash orphan"
        );

        let (removed, _bytes) =
            gc_orphans(&store, &gs, &keyed, false, &Default::default()).unwrap();
        assert_eq!(
            removed, 1,
            "exactly the orphan parquet is GC'd over real GCS"
        );
        let mut left = store.list_files(prefix).unwrap();
        left.sort();
        assert_eq!(
            left,
            vec![
                format!("{prefix}/manifest-r1.json"),
                format!("{prefix}/part-000000.parquet"),
            ],
            "the manifested part + its manifest survive the orphan GC"
        );

        drain(&store, prefix);
        assert!(
            store.list_files(prefix).unwrap().is_empty(),
            "teardown left the prefix clean over real GCS"
        );
    }
}
