//! **Layer: Execution** — the single home for committing one output part.

use std::path::Path;

use super::manifest_writer;
use super::summary::RunSummary;
use crate::destination::Destination;
use crate::error::Result;
use crate::journal::RunEvent;
use crate::plan::ResolvedRunPlan;
use crate::state::StateStore;

/// Adopt a part a PRIOR run committed (a resume) into the manifest and every run counter, once per path.
pub(in crate::pipeline) fn adopt_part(
    summary: &mut RunSummary,
    part: crate::manifest::ManifestPart,
) -> bool {
    if summary.manifest_parts.iter().any(|p| p.path == part.path) {
        return false;
    }
    summary.total_rows += part.rows;
    summary.bytes_written += part.size_bytes;
    summary.files_produced += 1;
    summary.files_committed += 1;
    summary.files_adopted += 1;
    summary.manifest_parts.push(part);
    true
}

/// ADR-0029: the COMMIT UNIT a durable part and its Form-B checksum contribution both belong to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum UnitId {
    /// The whole runner invocation.
    Run,
    /// A chunk window — the chunked runners (sequential / parallel / both checkpoint twins) and a `mongo_parallel` worker range.
    Chunk(i64),
    /// A keyset PAGE — the sequential keyset runner, which feeds the page's checksums and records the page's parts in the same loop iteration.
    Page(i64),
    /// A keyset RANGE — the parallel keyset runner (see the type doc).
    Range(i64),
}

/// ADR-0029 half 1 — facts that describe what the runner SAW.
#[derive(Debug, Clone, Default)]
pub(crate) struct Observations {
    /// The run's dest schema (first non-empty page/sink wins).
    pub(in crate::pipeline) drift_schema: Option<arrow::datatypes::Schema>,
    /// Max observed byte length per column (shape-drift warn input).
    pub(in crate::pipeline) column_max_bytes: std::collections::HashMap<String, u64>,
    /// What the sink measured against the `quality:` rules; graded once at the seam.
    pub(in crate::pipeline) quality: crate::quality::QualityTracker,
}

impl Observations {
    /// Fold another sink's observations in: first schema wins, shape max-merges, quality sums.
    pub(in crate::pipeline) fn merge(&mut self, other: Observations) {
        if self.drift_schema.is_none() {
            self.drift_schema = other.drift_schema;
        }
        for (col, len) in other.column_max_bytes {
            let e = self.column_max_bytes.entry(col).or_insert(0);
            *e = (*e).max(len);
        }
        self.quality.merge(other.quality);
    }
}

/// One commit unit's Form-B checksums and the key column they are keyed to.
#[derive(Debug, Clone, Default)]
pub(crate) struct UnitChecksums {
    pub(in crate::pipeline) sums: std::collections::BTreeMap<String, u64>,
    pub(in crate::pipeline) key: Option<String>,
}

/// ADR-0029 half 2 — the integrity record, which is only meaningful as a set covering EXACTLY the parts the manifest lists.
#[derive(Debug, Clone, Default)]
pub(crate) struct Integrity {
    /// Run-wide sum-combined per-column value checksums (Form B input).
    pub(in crate::pipeline) column_checksums: std::collections::BTreeMap<String, u64>,
    /// The key column the checksums are keyed to (first runner-reported wins).
    pub(in crate::pipeline) checksum_key_column: Option<String>,
    /// `manifest_parts.part_id` → the commit unit that produced it, written by [`record_part`] for every part it records.
    pub(in crate::pipeline) part_units: std::collections::BTreeMap<u32, UnitId>,
    /// The units that actually contributed checksums.
    pub(in crate::pipeline) covered_units: std::collections::BTreeSet<UnitId>,
}

/// ADR-0028: the run-wide tail ledger, filled by the runners as they commit and APPLIED once by [`crate::pipeline::finalize::finalize_export`].
#[derive(Debug, Clone, Default)]
pub(crate) struct CommitLedger {
    /// What the runner SAW — fed eagerly, applied on both paths.
    pub(in crate::pipeline) observed: Observations,
    /// What COMMITTED — commit-gated, coverage computed at harvest.
    pub(in crate::pipeline) integrity: Integrity,
}

impl CommitLedger {
    /// Feed what a runner SAW.
    pub(in crate::pipeline) fn observe(&mut self, o: Observations) {
        self.observed.merge(o);
    }

    /// Contribute one COMMITTED unit's checksums (see [`Self::contribute_checksums`]).
    pub(in crate::pipeline) fn contribute(&mut self, unit: UnitId, c: UnitChecksums) {
        self.contribute_checksums(unit, &c.sums, c.key);
    }

    /// ADR-0029: contribute one COMMITTED unit's Form-B checksums, keyed by the unit so the seam can compare them against the parts that unit recorded.
    pub(in crate::pipeline) fn contribute_checksums(
        &mut self,
        unit: UnitId,
        part: &std::collections::BTreeMap<String, u64>,
        key_column: Option<String>,
    ) {
        self.integrity.covered_units.insert(unit);
        accumulate_column_checksums(&mut self.integrity.column_checksums, part);
        if self.integrity.checksum_key_column.is_none() {
            self.integrity.checksum_key_column = key_column;
        }
    }

    /// ADR-0029: note which commit unit a just-recorded manifest part belongs to.
    fn note_part(&mut self, part_id: u32, unit: UnitId) {
        self.integrity.part_units.insert(part_id, unit);
    }
}

/// ADR-0029 — the computed coverage verdict for one run's Form-B record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::pipeline) struct Coverage {
    pub(in crate::pipeline) complete: bool,
    pub(in crate::pipeline) short_cover: bool,
    /// Parts recorded this run whose unit contributed no checksums.
    pub(in crate::pipeline) uncovered_parts: usize,
    /// Parts in the manifest that this run never recorded (rehydrated / cloned).
    pub(in crate::pipeline) foreign_parts: usize,
}

/// ADR-0029 — compare the contributions' unit set against the RECORDED PARTS' unit set, over the manifest itself.
pub(in crate::pipeline) fn compute_coverage(
    parts: &[crate::manifest::ManifestPart],
    integrity: &Integrity,
) -> Coverage {
    let mut uncovered_parts = 0usize;
    let mut foreign_parts = 0usize;
    for p in parts {
        match integrity.part_units.get(&p.part_id) {
            None => foreign_parts += 1,
            Some(unit) if !integrity.covered_units.contains(unit) => uncovered_parts += 1,
            Some(_) => {}
        }
    }
    Coverage {
        complete: uncovered_parts == 0 && foreign_parts == 0,
        short_cover: uncovered_parts > 0,
        uncovered_parts,
        foreign_parts,
    }
}

/// XOR-accumulate one part/page/chunk sink's per-column value checksums into a run-wide map.
pub(in crate::pipeline) fn accumulate_column_checksums(
    acc: &mut std::collections::BTreeMap<String, u64>,
    part: &std::collections::BTreeMap<String, u64>,
) {
    for (name, sum) in part {
        let e = acc.entry(name.clone()).or_insert(0);
        *e = e.wrapping_add(*sum);
    }
}

/// Record the run-wide sum-combined checksums + key column into the summary, so `finalize_manifest` writes Form B and `rivet validate` can re-verify the Arrow→Parquet encode / post-write fault Form A cannot see.
pub(in crate::pipeline) fn harvest_column_checksums(
    summary: &mut RunSummary,
    integrity: Integrity,
) {
    let cov = compute_coverage(&summary.manifest_parts, &integrity);
    if !cov.complete {
        summary.column_checksums_incomplete = true;
        summary.column_checksums_short_cover |= cov.short_cover;
        log::warn!(
            "export '{}': Form B value-checksums suppressed — the run-wide checksum covers \
             only part of this manifest ({} part(s) hydrated from a prior run and carrying no \
             per-column checksum, {} part(s) whose commit unit contributed none). `validate` \
             will size-verify parts but skip the Form B value re-read for this run.",
            summary.export_name,
            cov.foreign_parts,
            cov.uncovered_parts,
        );
        return;
    }
    if integrity.column_checksums.is_empty() {
        return;
    }
    summary.column_checksums = integrity
        .column_checksums
        .into_iter()
        .map(|(name, checksum)| crate::manifest::ColumnChecksum {
            name,
            checksum: checksum.to_string(),
        })
        .collect();
    summary.checksum_key_column = integrity.checksum_key_column;
}

/// A part written to the destination, ready to be recorded.
pub(crate) struct PartRecord {
    pub file_name: String,
    pub rows: i64,
    pub bytes: u64,
    pub fingerprint: String,
    /// Base64 MD5 of the part body (GCS `md5Hash` encoding), computed from the local temp file alongside the fingerprint.
    pub md5: String,
}

/// How a committed part is journaled.
pub(crate) enum PartKind {
    File {
        part_index: usize,
    },
    Chunk {
        chunk_index: i64,
    },
    Page {
        page_index: i64,
        /// The page's high-water key — written into the SAME file_log row as the part, so a crash-recovery resume reconciles the cursor from committed parts and never re-reads a committed page (v25 cursor-atomic keyset checkpoint).
        cursor_high: Option<String>,
    },
}

/// Seam 1 — ADR-0001 I1 + the destination-write boundary.
pub(crate) fn write_part_file(
    dest: &dyn Destination,
    tmp_path: &Path,
    rows: i64,
    file_name: String,
    landed: &std::sync::atomic::AtomicU64,
) -> Result<PartRecord> {
    let bytes = std::fs::metadata(tmp_path).map(|m| m.len()).unwrap_or(0);
    let outcome = dest.write(tmp_path, &file_name)?;
    landed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let (fingerprint, md5) =
        manifest_writer::compute_part_checksums(tmp_path).unwrap_or_else(|e| {
            log::warn!("part checksums failed for '{file_name}' (not fatal): {e:#}");
            (
                crate::manifest::SCHEMA_FINGERPRINT_UNAVAILABLE.to_string(),
                String::new(),
            )
        });
    if let Some(stored) = &outcome.content_md5 {
        use crate::pipeline::manifest_reconcile::md5_digest_bytes;
        if let (Some(local), Some(remote)) = (md5_digest_bytes(&md5), md5_digest_bytes(stored))
            && local != remote
        {
            crate::rivet_bail!(
                crate::error::codes::DEST_UPLOAD_CORRUPTED,
                "upload integrity check failed for '{file_name}': local MD5 differs from \
                 the store-reported checksum — the part corrupted in transit"
            );
        }
    }
    Ok(PartRecord {
        file_name,
        rows,
        bytes,
        fingerprint,
        md5,
    })
}

/// Seam 1a — finalize the sink's writer and write EVERY part it produced.
pub(crate) fn write_sink_parts(
    dest: &dyn Destination,
    sink: &mut crate::pipeline::sink::ExportSink,
    validate: Option<crate::config::FormatType>,
    name_for: impl Fn(usize, usize) -> String,
) -> (Vec<PartRecord>, Result<()>) {
    let parts = match sink.seal_parts() {
        Ok(p) => p,
        Err(e) => return (Vec::new(), Err(e)),
    };
    let count = parts.len();
    let mut written = Vec::with_capacity(count);
    for (idx, part) in parts.into_iter().enumerate() {
        let one = || -> Result<PartRecord> {
            crate::test_hook::maybe_error_at_index("sink_part_write", idx as i64)?;
            if let Some(fmt) = validate {
                crate::pipeline::validate::validate_output(part.tmp.path(), fmt, part.rows)?;
            }
            write_part_file(
                dest,
                part.tmp.path(),
                part.rows as i64,
                name_for(idx, count),
                &sink.parts_landed,
            )
        };
        match one() {
            Ok(rec) => written.push(rec),
            Err(e) => return (written, Err(e)),
        }
    }
    (written, Ok(()))
}

/// Sibling naming for rotated parts: a single-part chunk keeps its legacy name.
pub(crate) fn part_indexed_name(base: &str, idx: usize, count: usize) -> String {
    if count <= 1 {
        return base.to_string();
    }
    match base.rsplit_once('.') {
        Some((stem, ext)) => format!("{stem}_p{idx}.{ext}"),
        None => format!("{base}_p{idx}"),
    }
}

/// Seam 2 — the single home for the post-write ordering that used to drift across runners.
pub(crate) fn record_part(
    plan: &ResolvedRunPlan,
    summary: &mut RunSummary,
    state: Option<&StateStore>,
    part: &PartRecord,
    kind: PartKind,
    unit: UnitId,
) -> bool {
    crate::test_hook::maybe_panic_at("after_file_write");

    let recorded = manifest_writer::record_committed_part_with_fingerprint(
        summary,
        part.file_name.clone(),
        part.rows,
        part.bytes,
        part.fingerprint.clone(),
        part.md5.clone(),
    );
    let deduped = recorded.deduped;
    if !deduped {
        summary.total_rows += part.rows;
        summary.bytes_written += part.bytes;
        summary.files_produced += 1;
        summary.files_committed += 1;
    }
    summary.ledger.note_part(recorded.part_id, unit);

    match &kind {
        PartKind::File { part_index } => summary.journal.record(RunEvent::FileWritten {
            file_name: part.file_name.clone(),
            rows: part.rows,
            bytes: part.bytes,
            part_index: *part_index,
        }),
        PartKind::Chunk { chunk_index } => summary.journal.record(RunEvent::ChunkCompleted {
            chunk_index: *chunk_index,
            rows: part.rows,
            file_name: Some(part.file_name.clone()),
        }),
        PartKind::Page { page_index, .. } => summary.journal.record(RunEvent::ChunkCompleted {
            chunk_index: *page_index,
            rows: part.rows,
            file_name: Some(part.file_name.clone()),
        }),
    }

    let cursor_high: Option<&str> = match &kind {
        PartKind::Page { cursor_high, .. } => cursor_high.as_deref(),
        _ => None,
    };

    if let Some(st) = state
        && let Err(e) = st.record_durable_part(crate::state::DurablePart {
            run_id: &summary.run_id,
            export_name: &plan.export_name,
            file_name: &part.file_name,
            rows: part.rows,
            bytes: part.bytes as i64,
            format: plan.format.label(),
            compression: Some(plan.compression.label()),
            mode: plan.strategy.mode_label(),
            cursor_high,
        })
    {
        log::warn!(
            "export '{}': file_log write failed for '{}' (file was produced): {:#}",
            plan.export_name,
            part.file_name,
            e
        );
    }

    crate::test_hook::maybe_panic_at("after_manifest_update");
    deduped
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::config::{DestinationConfig, DestinationType};
    use crate::destination::local::LocalDestination;
    use crate::journal::RunEvent;
    use crate::pipeline::summary::RunSummary;
    use crate::plan::ResolvedRunPlan;
    use crate::plan::test_support::test_plan;
    use crate::state::StateStore;
    use std::io::Write;

    /// ADR-0028 CommitLedger semantics, each of which a runner relies on.
    #[test]
    fn drain_tail_into_moves_schema_checksums_key_and_shape_to_the_ledger() {
        use arrow::datatypes::{DataType, Field, Schema};
        let plan = test_plan();
        let mut sink = crate::pipeline::sink::ExportSink::new(&plan).expect("sink");
        sink.dest_schema = Some(std::sync::Arc::new(Schema::new(vec![Field::new(
            "id",
            DataType::Int64,
            false,
        )])));
        sink.column_checksums = [("id".to_string(), 41u64)].into();
        sink.checksum_key_col = Some(0);
        sink.cursor_column = Some("id".to_string());
        sink.column_max_bytes = [("id".to_string(), 8u64)].into();
        sink.quality.null_counts = [("id".to_string(), 3usize)].into();

        let mut led = CommitLedger::default();
        sink.drain_observations_into(&mut led);
        led.contribute(UnitId::Run, sink.take_checksums());

        assert_eq!(
            led.observed
                .drift_schema
                .as_ref()
                .map(|s| s.field(0).name().clone()),
            Some("id".to_string()),
            "the sink's dest schema must reach the ledger"
        );
        assert_eq!(led.integrity.column_checksums.get("id"), Some(&41u64));
        assert_eq!(led.integrity.checksum_key_column.as_deref(), Some("id"));
        assert_eq!(led.observed.column_max_bytes.get("id"), Some(&8u64));
        assert_eq!(
            led.observed.quality.null_counts.get("id"),
            Some(&3usize),
            "the sink's quality measurements must reach the ledger"
        );
        assert!(
            led.integrity.covered_units.contains(&UnitId::Run),
            "the integrity drain must REGISTER its commit unit, or the seam reads \
             every part single wrote as uncovered and suppresses Form B"
        );
        assert!(
            sink.column_checksums.is_empty()
                && sink.column_max_bytes.is_empty()
                && sink.quality.null_counts.is_empty(),
            "drain must TAKE the sink's accumulators, not copy them"
        );
        sink.quality.null_counts = [("id".to_string(), 5usize)].into();
        assert_eq!(
            sink.take_shape().quality.null_counts.get("id"),
            Some(&5usize),
            "the chunked runners' feed must carry the quality measurements too"
        );
    }

    #[test]
    fn commit_ledger_first_wins_schema_and_key_and_commutative_merges() {
        use arrow::datatypes::{DataType, Field, Schema};
        let mut led = CommitLedger::default();

        let a = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        let b = Schema::new(vec![Field::new("other", DataType::Utf8, true)]);
        let obs = |sc: &Schema| Observations {
            drift_schema: Some(sc.clone()),
            ..Default::default()
        };
        led.observe(obs(&a));
        led.observe(obs(&b));
        assert_eq!(
            led.observed
                .drift_schema
                .as_ref()
                .map(|s| s.field(0).name().clone()),
            Some("id".to_string()),
            "observe is first-wins on the schema — a later page/worker schema must not replace the run's"
        );

        let p1: std::collections::BTreeMap<String, u64> = [("v".to_string(), 7u64)].into();
        let p2: std::collections::BTreeMap<String, u64> = [("v".to_string(), 5u64)].into();
        led.contribute_checksums(UnitId::Chunk(0), &Default::default(), None);
        led.contribute_checksums(UnitId::Chunk(1), &p1, Some("id".into()));
        led.contribute_checksums(UnitId::Chunk(2), &p2, Some("late".into()));
        assert_eq!(led.integrity.checksum_key_column.as_deref(), Some("id"));
        assert_eq!(
            led.integrity.column_checksums.get("v"),
            Some(&12u64),
            "checksum merge must fold (wrapping add), not overwrite"
        );
        assert_eq!(
            led.integrity.covered_units,
            [UnitId::Chunk(0), UnitId::Chunk(1), UnitId::Chunk(2)]
                .into_iter()
                .collect::<std::collections::BTreeSet<_>>(),
            "an empty contribution still covers its unit"
        );

        let s1: std::collections::HashMap<String, u64> = [("t".to_string(), 100u64)].into();
        let s2: std::collections::HashMap<String, u64> = [("t".to_string(), 40u64)].into();
        for column_max_bytes in [s1, s2] {
            led.observe(Observations {
                column_max_bytes,
                ..Default::default()
            });
        }
        assert_eq!(led.observed.column_max_bytes.get("t"), Some(&100u64));
    }

    pub(crate) fn test_summary(plan: &ResolvedRunPlan) -> RunSummary {
        let mut s = RunSummary::stub_for_testing("test_run", plan.export_name.clone());
        s.batch_size = 10_000;
        s.mode = "snapshot".into();
        s.compression = "none".into();
        s
    }

    fn test_part(file_name: &str) -> PartRecord {
        PartRecord {
            file_name: file_name.into(),
            rows: 42,
            bytes: 1024,
            fingerprint: "xxh3:1234567890abcdef".into(),
            md5: String::new(),
        }
    }

    #[test]
    fn part_indexed_name_keeps_legacy_name_for_single_part_and_suffixes_siblings() {
        assert_eq!(
            part_indexed_name("orders_ts_chunk3.parquet", 0, 1),
            "orders_ts_chunk3.parquet"
        );
        assert_eq!(
            part_indexed_name("orders_ts_chunk3.parquet", 0, 3),
            "orders_ts_chunk3_p0.parquet"
        );
        assert_eq!(
            part_indexed_name("orders_ts_chunk3.parquet", 2, 3),
            "orders_ts_chunk3_p2.parquet"
        );
        assert_eq!(part_indexed_name("orders_chunk3", 1, 2), "orders_chunk3_p1");
    }

    #[test]
    fn write_part_file_copies_to_destination_and_returns_real_bytes_and_fingerprint() {
        let src_dir = tempfile::tempdir().unwrap();
        let dst_dir = tempfile::tempdir().unwrap();
        let src_path = src_dir.path().join("part.parquet");
        let payload: &[u8] = b"hello rivet";
        std::fs::File::create(&src_path)
            .unwrap()
            .write_all(payload)
            .unwrap();

        let dest = LocalDestination::new(&DestinationConfig {
            destination_type: DestinationType::Local,
            path: Some(dst_dir.path().to_string_lossy().into_owned()),
            ..Default::default()
        })
        .unwrap();

        let landed = std::sync::atomic::AtomicU64::new(0);
        let rec = write_part_file(&dest, &src_path, 7, "out/part.parquet".into(), &landed)
            .expect("write ok");
        assert_eq!(landed.into_inner(), 1, "the part is at the destination");

        assert_eq!(rec.file_name, "out/part.parquet");
        assert_eq!(rec.rows, 7);
        assert_eq!(rec.bytes, payload.len() as u64);
        assert!(
            rec.fingerprint.starts_with("xxh3:") && rec.fingerprint.len() == 21,
            "fingerprint should be xxh3:<16 hex chars>, got {:?}",
            rec.fingerprint
        );
        let written = dst_dir.path().join("out").join("part.parquet");
        assert_eq!(std::fs::read(&written).unwrap(), payload);
    }

    /// A destination that accepts the first `ok` writes and refuses the rest.
    pub(crate) struct NthWriteFails {
        ok: usize,
        seen: std::sync::atomic::AtomicUsize,
    }
    impl NthWriteFails {
        /// Accept `ok` writes, then refuse every one after.
        pub(crate) fn after(ok: usize) -> Self {
            Self {
                ok,
                seen: Default::default(),
            }
        }
    }
    impl crate::destination::Destination for NthWriteFails {
        fn write(
            &self,
            _p: &std::path::Path,
            key: &str,
        ) -> Result<crate::destination::WriteOutcome> {
            if self.seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst) >= self.ok {
                return Err(anyhow::anyhow!("injected: write of '{key}' refused"));
            }
            Ok(crate::destination::WriteOutcome { content_md5: None })
        }
        fn capabilities(&self) -> crate::destination::DestinationCapabilities {
            crate::destination::DestinationCapabilities {
                commit_protocol: crate::destination::WriteCommitProtocol::FinalizeOnClose,
                idempotent_overwrite: true,
                retry_safe: true,
                partial_write_risk: false,
            }
        }
    }

    /// A destination that reports a fixed content checksum (no real upload).
    struct ChecksumDest(Option<String>);
    impl crate::destination::Destination for ChecksumDest {
        fn write(&self, _p: &Path, _k: &str) -> Result<crate::destination::WriteOutcome> {
            Ok(crate::destination::WriteOutcome {
                content_md5: self.0.clone(),
            })
        }
        fn capabilities(&self) -> crate::destination::DestinationCapabilities {
            crate::destination::DestinationCapabilities {
                commit_protocol: crate::destination::WriteCommitProtocol::FinalizeOnClose,
                idempotent_overwrite: true,
                retry_safe: true,
                partial_write_risk: false,
            }
        }
    }

    fn stage(payload: &[u8]) -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("part.parquet");
        std::fs::write(&p, payload).unwrap();
        (dir, p)
    }

    #[test]
    fn transit_check_bails_when_store_checksum_differs() {
        let (_d, src) = stage(b"hello rivet");
        let dest = ChecksumDest(Some("AAAAAAAAAAAAAAAAAAAAAA==".into()));
        let landed = std::sync::atomic::AtomicU64::new(0);
        match write_part_file(&dest, &src, 1, "part.parquet".into(), &landed) {
            Ok(_) => panic!("mismatched checksum must fail"),
            Err(e) => assert!(
                e.to_string().contains("transit")
                    && crate::error::error_code(&e) == Some("RIVET_DEST_UPLOAD_CORRUPTED")
                    && crate::error::classify_exit(&e) == 2,
                "expected a coded transit-corruption error, got: {e}"
            ),
        }
        assert_eq!(
            landed.into_inner(),
            1,
            "the corrupt part is at the destination, in no manifest: the run did write"
        );
    }

    #[test]
    fn a_write_the_destination_refused_put_no_part_there() {
        let (_d, src) = stage(b"hello rivet");
        let landed = std::sync::atomic::AtomicU64::new(0);
        let refused = write_part_file(
            &NthWriteFails::after(0),
            &src,
            1,
            "p.parquet".into(),
            &landed,
        );
        assert!(refused.is_err(), "the destination refused the write");
        assert_eq!(landed.into_inner(), 0);
    }

    #[test]
    fn transit_check_passes_on_match_and_when_store_is_silent() {
        use base64::Engine as _;
        use md5::{Digest, Md5};
        let payload = b"hello rivet";
        let (_d, src) = stage(payload);
        let mut h = Md5::new();
        h.update(payload);
        let real = base64::engine::general_purpose::STANDARD.encode(h.finalize());
        let landed = Default::default();
        write_part_file(
            &ChecksumDest(Some(real)),
            &src,
            1,
            "p.parquet".into(),
            &landed,
        )
        .expect("matching checksum passes");
        write_part_file(&ChecksumDest(None), &src, 1, "p.parquet".into(), &landed)
            .expect("silent store passes");
    }

    #[test]
    fn record_part_file_kind_bumps_counters_and_journals_file_written() {
        let plan = test_plan();
        let mut summary = test_summary(&plan);
        let part = test_part("orders_chunk0.parquet");

        record_part(
            &plan,
            &mut summary,
            None,
            &part,
            PartKind::File { part_index: 0 },
            UnitId::Run,
        );

        assert_eq!(summary.bytes_written, part.bytes);
        assert_eq!(summary.files_produced, 1);
        assert_eq!(summary.files_committed, 1);
        assert_eq!(summary.manifest_parts.len(), 1);
        assert_eq!(summary.manifest_parts[0].path, part.file_name);
        assert_eq!(summary.manifest_parts[0].rows, part.rows);

        let file_events = summary.journal.files();
        assert_eq!(file_events.len(), 1, "must journal one FileWritten");
        assert!(
            matches!(
                &file_events[0].event,
                RunEvent::FileWritten { part_index: 0, .. }
            ),
            "expected FileWritten{{part_index:0}}, got {:?}",
            file_events[0].event
        );
        assert!(
            summary.journal.chunk_events().is_empty(),
            "File kind must not journal ChunkCompleted"
        );
    }

    #[test]
    fn record_part_chunk_kind_journals_chunk_completed_with_file_name() {
        let plan = test_plan();
        let mut summary = test_summary(&plan);
        let part = test_part("orders_chunk7.parquet");

        record_part(
            &plan,
            &mut summary,
            None,
            &part,
            PartKind::Chunk { chunk_index: 7 },
            UnitId::Chunk(7),
        );

        let events = summary.journal.chunk_events();
        assert_eq!(events.len(), 1, "must journal one ChunkCompleted");
        match &events[0].event {
            RunEvent::ChunkCompleted {
                chunk_index,
                rows,
                file_name,
            } => {
                assert_eq!(*chunk_index, 7);
                assert_eq!(*rows, part.rows);
                assert_eq!(file_name.as_deref(), Some(part.file_name.as_str()));
            }
            other => panic!("expected ChunkCompleted, got {other:?}"),
        }
        assert!(
            summary.journal.files().is_empty(),
            "Chunk kind must not journal FileWritten"
        );
    }

    #[test]
    fn record_part_with_state_writes_file_log_entry() {
        let plan = test_plan();
        let mut summary = test_summary(&plan);
        let state = StateStore::open_in_memory().expect("in-memory state");
        let part = test_part("orders_chunk0.parquet");

        record_part(
            &plan,
            &mut summary,
            Some(&state),
            &part,
            PartKind::Chunk { chunk_index: 0 },
            UnitId::Chunk(0),
        );

        let files = state.get_files(Some(&plan.export_name), 16).unwrap();
        assert_eq!(files.len(), 1, "I7: file_log must carry exactly one entry");
        assert_eq!(files[0].file_name, part.file_name);
        assert_eq!(files[0].row_count, part.rows);
    }

    #[test]
    fn record_part_with_none_state_is_a_bypass_not_a_failure() {
        let plan = test_plan();
        let mut summary = test_summary(&plan);
        let part = test_part("orders_chunk0.parquet");

        record_part(
            &plan,
            &mut summary,
            None,
            &part,
            PartKind::Chunk { chunk_index: 0 },
            UnitId::Chunk(0),
        );

        assert_eq!(summary.files_committed, 1);
        assert_eq!(summary.manifest_parts.len(), 1);
        assert_eq!(summary.journal.chunk_events().len(), 1);
    }

    pub(crate) fn synthetic_parts(n: usize) -> Vec<PartRecord> {
        (0..n)
            .map(|i| PartRecord {
                file_name: format!("part_{i}.parquet"),
                rows: 100 + (i as i64) * 10,
                bytes: 1024 * ((i as u64) + 1),
                fingerprint: format!("xxh3:{i:016x}"),
                md5: String::new(),
            })
            .collect()
    }

    #[test]
    fn adopt_part_counts_a_prior_part_once_and_marks_it_adopted() {
        let plan = test_plan();
        let mut summary = test_summary(&plan);
        let part = crate::manifest::ManifestPart {
            part_id: 1,
            path: "p0.parquet".into(),
            rows: 50,
            size_bytes: 10,
            content_fingerprint: String::new(),
            content_md5: String::new(),
            status: crate::manifest::PartStatus::Committed,
        };
        assert!(adopt_part(&mut summary, part.clone()));
        assert!(
            !adopt_part(&mut summary, part),
            "the same path is adopted once"
        );
        assert_eq!(
            (
                summary.total_rows,
                summary.files_committed,
                summary.files_adopted
            ),
            (50, 1, 1)
        );
        assert_eq!(
            (summary.bytes_written, summary.manifest_parts.len()),
            (10, 1)
        );
        assert_eq!(
            summary.files_committed_here(),
            0,
            "adopted parts were not written here"
        );
    }

    #[test]
    fn record_part_keeps_summary_aggregates_coherent_with_manifest_parts() {
        let plan = test_plan();
        let mut summary = test_summary(&plan);
        let parts = synthetic_parts(5);

        for (i, p) in parts.iter().enumerate() {
            record_part(
                &plan,
                &mut summary,
                None,
                p,
                PartKind::Chunk {
                    chunk_index: i as i64,
                },
                UnitId::Chunk(i as i64),
            );
        }

        let parts_rows: i64 = summary.manifest_parts.iter().map(|p| p.rows).sum();
        let parts_bytes: u64 = summary.manifest_parts.iter().map(|p| p.size_bytes).sum();

        assert_eq!(
            summary.total_rows, parts_rows,
            "non-resume run: summary.total_rows must equal sum(manifest_parts.rows)"
        );
        assert_eq!(
            summary.bytes_written, parts_bytes,
            "non-resume run: summary.bytes_written must equal sum(manifest_parts.size_bytes)"
        );
        assert_eq!(
            summary.files_produced,
            summary.manifest_parts.len(),
            "files_produced must equal manifest_parts.len() (record_part bumps both)"
        );
        assert_eq!(
            summary.files_committed,
            summary.manifest_parts.len(),
            "files_committed must equal manifest_parts.len() (record_part bumps both)"
        );
    }

    #[test]
    fn nonempty_successful_run_must_have_nonempty_manifest_parts() {
        let plan = test_plan();
        let mut summary = test_summary(&plan);
        let part = test_part("orders_chunk0.parquet");

        summary.total_rows += part.rows;
        record_part(
            &plan,
            &mut summary,
            None,
            &part,
            PartKind::Chunk { chunk_index: 0 },
            UnitId::Chunk(0),
        );
        summary.status = "success".into();

        assert!(summary.files_committed > 0, "test premise: work committed");
        assert!(
            !summary.manifest_parts.is_empty(),
            "non-empty success run must surface files in manifest_parts"
        );
        assert_eq!(
            summary.files_committed,
            summary.manifest_parts.len(),
            "files_committed and manifest_parts.len() locked together by record_part"
        );
    }

    /// Feed the ledger the way a runner does: N parts under one commit unit, then that unit's checksums.
    fn run_one_unit(plan: &ResolvedRunPlan, unit: UnitId, parts: &[PartRecord]) -> RunSummary {
        let mut summary = test_summary(plan);
        for p in parts {
            record_part(
                plan,
                &mut summary,
                None,
                p,
                PartKind::Chunk { chunk_index: 0 },
                unit,
            );
        }
        summary.ledger.contribute_checksums(
            unit,
            &[("id".to_string(), 0xdead_beefu64)].into(),
            Some("id".into()),
        );
        summary
    }

    /// THE positive.
    #[test]
    fn harvest_records_form_b_when_every_manifest_part_s_unit_contributed() {
        let plan = test_plan();
        let parts = synthetic_parts(3);
        let mut summary = run_one_unit(&plan, UnitId::Chunk(0), &parts);
        assert_eq!(summary.manifest_parts.len(), 3, "fixture: 3 parts recorded");

        let integrity = std::mem::take(&mut summary.ledger).integrity;
        harvest_column_checksums(&mut summary, integrity);

        assert_eq!(
            summary
                .column_checksums
                .iter()
                .map(|c| (c.name.as_str(), c.checksum.as_str()))
                .collect::<Vec<_>>(),
            vec![("id", "3735928559")],
            "a fully-covered run must RECORD Form B, not merely fail to suppress it"
        );
        assert_eq!(summary.checksum_key_column.as_deref(), Some("id"));
        assert!(
            !summary.column_checksums_incomplete && !summary.column_checksums_short_cover,
            "a fully-covered run must be flagged neither incomplete nor short-cover"
        );
    }

    /// A part RECORDED by this run whose unit contributed nothing.
    #[test]
    fn harvest_suppresses_form_b_when_a_recorded_part_s_unit_contributed_nothing() {
        let plan = test_plan();
        let parts = synthetic_parts(2);
        let mut summary = run_one_unit(&plan, UnitId::Chunk(0), &parts);
        let orphan = PartRecord {
            file_name: "uncommitted_range.parquet".into(),
            rows: 10,
            bytes: 64,
            fingerprint: "xxh3:000000000000dead".into(),
            md5: String::new(),
        };
        record_part(
            &plan,
            &mut summary,
            None,
            &orphan,
            PartKind::Chunk { chunk_index: 1 },
            UnitId::Chunk(1),
        );

        let integrity = std::mem::take(&mut summary.ledger).integrity;
        harvest_column_checksums(&mut summary, integrity);

        assert!(
            summary.column_checksums.is_empty(),
            "one uncovered part must suppress the WHOLE Form-B record, never publish a \
             checksum covering a subset of the manifest"
        );
        assert!(summary.column_checksums_incomplete);
        assert!(
            summary.column_checksums_short_cover,
            "a part this run RECORDED but did not cover is the never-legitimate half — it \
             must be distinguishable from a resume hydration, or the invariant cannot fire"
        );
    }

    /// The resume case, which ADR-0029 SUBSUMES.
    #[test]
    fn harvest_suppresses_form_b_for_a_hydrated_part_with_no_commit_unit() {
        let plan = test_plan();
        let parts = synthetic_parts(2);
        let mut summary = run_one_unit(&plan, UnitId::Chunk(0), &parts);
        summary.manifest_parts.push(crate::manifest::ManifestPart {
            part_id: 99,
            path: "precrash_chunk7.parquet".into(),
            rows: 100,
            size_bytes: 4096,
            content_fingerprint: String::new(),
            content_md5: String::new(),
            status: crate::manifest::PartStatus::Committed,
        });

        let integrity = std::mem::take(&mut summary.ledger).integrity;
        harvest_column_checksums(&mut summary, integrity);

        assert!(
            summary.column_checksums.is_empty(),
            "a hydrated pre-crash part carries no per-column checksum, so the run-wide record \
             cannot cover the manifest — suppress, never publish a partial"
        );
        assert!(summary.column_checksums_incomplete);
        assert!(
            !summary.column_checksums_short_cover,
            "hydration is LEGITIMATE — flagging it short-cover would fail every crash-recovery \
             resume through check_post_run_invariants"
        );
    }

    /// The same hydration case with an EMPTY accumulator.
    #[test]
    fn harvest_flags_a_hydration_only_resume_that_re_ran_nothing_and_has_no_checksums() {
        let plan = test_plan();
        let mut summary = test_summary(&plan);
        summary.manifest_parts.push(crate::manifest::ManifestPart {
            part_id: 1,
            path: "precrash_chunk0.parquet".into(),
            rows: 100,
            size_bytes: 4096,
            content_fingerprint: String::new(),
            content_md5: String::new(),
            status: crate::manifest::PartStatus::Committed,
        });
        summary.files_committed = 1;
        summary.files_produced = 1;

        let integrity = std::mem::take(&mut summary.ledger).integrity;
        assert!(
            integrity.column_checksums.is_empty(),
            "fixture: no checksums"
        );
        harvest_column_checksums(&mut summary, integrity);

        assert!(
            summary.column_checksums_incomplete,
            "a resume that hydrated parts and re-ran nothing must still RECORD the \
             suppression, or the Form-B telltale reads an unexplained empty record and \
             panics a legitimate crash-recovery run"
        );
        assert!(!summary.column_checksums_short_cover);
        summary.state_backed = true;
        summary.status = "success".into();
        summary.format = "parquet".into();
        summary.schema_changed = Some(false);
        assert!(
            summary.check_post_run_invariants(true).is_ok(),
            "and the telltales must then PASS the resume: {:?}",
            summary.check_post_run_invariants(true)
        );
    }

    /// A hydrated part that this run RE-READ and overwrote (the keyset mid-page-crash fallback) dedupes in place, keeping its part_id.
    #[test]
    fn a_deduped_re_read_of_a_hydrated_part_becomes_covered() {
        let plan = test_plan();
        let mut summary = test_summary(&plan);
        summary.manifest_parts.push(crate::manifest::ManifestPart {
            part_id: 1,
            path: "part_0.parquet".into(),
            rows: 100,
            size_bytes: 1024,
            content_fingerprint: String::new(),
            content_md5: String::new(),
            status: crate::manifest::PartStatus::Committed,
        });
        let parts = synthetic_parts(1);
        let deduped = record_part(
            &plan,
            &mut summary,
            None,
            &parts[0],
            PartKind::Chunk { chunk_index: 0 },
            UnitId::Chunk(0),
        );
        assert!(deduped, "fixture: the re-read must dedup, not append");
        assert_eq!(summary.manifest_parts.len(), 1);
        summary.ledger.contribute_checksums(
            UnitId::Chunk(0),
            &[("id".to_string(), 7u64)].into(),
            Some("id".into()),
        );

        let integrity = std::mem::take(&mut summary.ledger).integrity;
        harvest_column_checksums(&mut summary, integrity);

        assert!(
            !summary.column_checksums.is_empty(),
            "a re-read part IS covered by this run's checksums — suppressing here would make \
             every mid-page-crash resume lose Form B for no reason"
        );
    }

    /// `compute_coverage` counts BOTH shortfall kinds independently, over a mixed manifest.
    #[test]
    fn compute_coverage_counts_foreign_and_uncovered_parts_separately() {
        let part = |id: u32| crate::manifest::ManifestPart {
            part_id: id,
            path: format!("p{id}.parquet"),
            rows: 1,
            size_bytes: 1,
            content_fingerprint: String::new(),
            content_md5: String::new(),
            status: crate::manifest::PartStatus::Committed,
        };
        let mut integrity = Integrity::default();
        for id in [1u32, 2] {
            integrity.part_units.insert(id, UnitId::Chunk(0));
        }
        for id in [3u32, 4] {
            integrity.part_units.insert(id, UnitId::Chunk(1));
        }
        integrity.covered_units.insert(UnitId::Chunk(0));
        let parts: Vec<_> = (1u32..=6).map(part).collect();

        let cov = compute_coverage(&parts, &integrity);
        assert_eq!(
            (
                cov.complete,
                cov.short_cover,
                cov.uncovered_parts,
                cov.foreign_parts
            ),
            (false, true, 2, 2)
        );

        integrity.covered_units.insert(UnitId::Chunk(1));
        let cov = compute_coverage(&parts[..4], &integrity);
        assert_eq!(
            (
                cov.complete,
                cov.short_cover,
                cov.uncovered_parts,
                cov.foreign_parts
            ),
            (true, false, 0, 0)
        );
    }

    /// The unit-id MISMATCH, end to end through the two real seams.
    #[test]
    fn a_unit_id_mismatch_suppresses_form_b_and_fails_the_run_instead_of_going_silent() {
        let plan = test_plan();
        let parts = synthetic_parts(1);
        let mut summary = test_summary(&plan);
        record_part(
            &plan,
            &mut summary,
            None,
            &parts[0],
            PartKind::Chunk { chunk_index: 0 },
            UnitId::Chunk(0),
        );
        summary.ledger.contribute_checksums(
            UnitId::Chunk(1),
            &[("id".to_string(), 1u64)].into(),
            None,
        );
        let integrity = std::mem::take(&mut summary.ledger).integrity;
        harvest_column_checksums(&mut summary, integrity);
        assert!(
            summary.column_checksums.is_empty() && summary.column_checksums_short_cover,
            "a UnitId mismatch must suppress AND reach the flag check_post_run_invariants reads"
        );

        summary.state_backed = true;
        summary.status = "success".into();
        summary.format = "parquet".into();
        summary.schema_changed = Some(false);
        let verdict = summary.check_post_run_invariants(false);
        assert!(
            verdict
                .as_ref()
                .err()
                .is_some_and(|m| m.contains("UnitId mismatch")),
            "the backstop must FAIL a successful state_backed run carrying short coverage, \
             naming the mismatch; got {verdict:?}"
        );
    }
}
