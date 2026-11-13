//! **Layer: Planning** (decision-only, no I/O)

use std::collections::BTreeMap;

use crate::destination::ObjectMeta;
use crate::manifest::RunManifest;
use crate::pipeline::manifest_reconcile::{PartPresence, reconcile_manifest_against_listing};

/// What `--resume` should do with a single part on the next run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResumeDecision {
    /// Manifest entry exists, object exists, size matches, fingerprint matches → already committed.
    Skip,
    /// Manifest entry exists but the object is missing at the destination (e.g.
    Rewrite,
    /// Manifest entry exists, object exists, but `size_bytes` differs from the recorded value (or the listing surfaced a fingerprint drift the caller decoded into the args).
    Quarantine { reason: QuarantineReason },
}

/// What to do with an object that exists at the destination but has no manifest entry pointing at it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UntrackedDecision {
    /// Move to `<prefix>/_quarantine/<run_id>/<original-name>` (best-effort).
    Quarantine,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuarantineReason {
    /// `size_bytes` on the destination differs from the manifest entry.
    SizeMismatch,
    /// The object's content MD5 (from the listing metadata) diverged from the manifest's.
    FingerprintMismatch,
}

/// Aggregate output of one matrix evaluation.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ResumePlan {
    /// Per-part decisions, keyed by `manifest.parts[].path`.
    pub per_part: BTreeMap<String, PartDecision>,
    /// Surplus objects under the prefix that are not referenced by any committed manifest entry (M9 untracked).
    pub untracked: BTreeMap<String, UntrackedDecision>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartDecision {
    pub part_id: u32,
    pub decision: ResumeDecision,
}

impl ResumePlan {
    /// Number of parts whose decision is `Skip`.
    pub fn skipped(&self) -> usize {
        self.per_part
            .values()
            .filter(|d| matches!(d.decision, ResumeDecision::Skip))
            .count()
    }

    /// Number of parts the executor must re-export.
    pub fn rewrites(&self) -> usize {
        self.per_part
            .values()
            .filter(|d| matches!(d.decision, ResumeDecision::Rewrite))
            .count()
    }

    /// Number of parts marked for quarantine (size or fingerprint drift).
    pub fn quarantines(&self) -> usize {
        self.per_part
            .values()
            .filter(|d| matches!(d.decision, ResumeDecision::Quarantine { .. }))
            .count()
    }
}

/// Apply ADR-0012 M8's decision matrix.
pub fn build_resume_plan(manifest: &RunManifest, listing: &[ObjectMeta]) -> ResumePlan {
    let rec = reconcile_manifest_against_listing(manifest, listing, "");

    let mut plan = ResumePlan::default();
    for check in rec.per_part {
        let decision = match check.presence {
            PartPresence::Present { .. } => ResumeDecision::Skip,
            PartPresence::Missing => ResumeDecision::Rewrite,
            PartPresence::SizeMismatch { .. } => ResumeDecision::Quarantine {
                reason: QuarantineReason::SizeMismatch,
            },
            PartPresence::ChecksumMismatch { .. } => ResumeDecision::Quarantine {
                reason: QuarantineReason::FingerprintMismatch,
            },
        };
        plan.per_part.insert(
            check.path,
            PartDecision {
                part_id: check.part_id,
                decision,
            },
        );
    }
    for obj in rec.untracked {
        plan.untracked
            .insert(obj.key, UntrackedDecision::Quarantine);
    }
    plan
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::{
        MANIFEST_FILENAME, MANIFEST_VERSION, ManifestDestination, ManifestPart, ManifestSource,
        ManifestStatus, PartStatus, RunManifest, SUCCESS_FILENAME,
    };

    fn part(part_id: u32, path: &str, size: u64) -> ManifestPart {
        ManifestPart {
            part_id,
            path: path.into(),
            rows: 100,
            size_bytes: size,
            content_fingerprint: format!("xxh3:{:016x}", part_id as u64),
            content_md5: String::new(),
            status: PartStatus::Committed,
        }
    }

    fn manifest(parts: Vec<ManifestPart>) -> RunManifest {
        let row_count = parts
            .iter()
            .filter(|p| p.status == PartStatus::Committed)
            .map(|p| p.rows)
            .sum();
        let part_count = parts
            .iter()
            .filter(|p| p.status == PartStatus::Committed)
            .count() as u32;
        RunManifest {
            split_window: None,
            checksum_render: None,
            row_hash: None,
            mode: "batch".to_string(),
            manifest_version: MANIFEST_VERSION,
            run_id: "r1".into(),
            export_name: "public.orders".into(),
            export_family: String::new(),
            started_at: "2026-05-21T12:00:00Z".into(),
            finished_at: "2026-05-21T12:01:00Z".into(),
            status: ManifestStatus::Success,
            source: ManifestSource {
                engine: "postgres".into(),
                schema: Some("public".into()),
                table: Some("orders".into()),
                extraction: None,
            },
            destination: ManifestDestination {
                kind: "local".into(),
                uri: "file:///tmp/out".into(),
            },
            format: "parquet".into(),
            compression: "zstd".into(),
            schema_fingerprint: "xxh3:0000000000000000".into(),
            row_count,
            part_count,
            parts,
            column_checksums: None,
            checksum_key_column: None,
        }
    }

    fn obj(key: &str, size: u64) -> ObjectMeta {
        ObjectMeta {
            key: key.into(),
            size_bytes: size,
            content_md5: None,
        }
    }

    /// Row: manifest yes + obj yes + size match → skip (committed).
    #[test]
    fn part_present_with_matching_size_is_skipped() {
        let m = manifest(vec![part(1, "part-000001.parquet", 4096)]);
        let listing = vec![obj("part-000001.parquet", 4096)];
        let plan = build_resume_plan(&m, &listing);
        assert_eq!(plan.skipped(), 1);
        assert_eq!(plan.rewrites(), 0);
        assert_eq!(plan.quarantines(), 0);
        assert_eq!(
            plan.per_part["part-000001.parquet"].decision,
            ResumeDecision::Skip
        );
    }

    /// Row: manifest yes + obj yes + size mismatch → quarantine (M9).
    #[test]
    fn part_present_with_size_drift_is_quarantined() {
        let m = manifest(vec![part(1, "part-000001.parquet", 4096)]);
        let listing = vec![obj("part-000001.parquet", 9999)];
        let plan = build_resume_plan(&m, &listing);
        assert_eq!(plan.quarantines(), 1);
        assert_eq!(
            plan.per_part["part-000001.parquet"].decision,
            ResumeDecision::Quarantine {
                reason: QuarantineReason::SizeMismatch
            }
        );
    }

    /// Row: manifest yes + obj missing → rewrite (lost).
    #[test]
    fn part_missing_from_destination_is_rewritten() {
        let m = manifest(vec![
            part(1, "part-000001.parquet", 4096),
            part(2, "part-000002.parquet", 8192),
        ]);
        let listing = vec![obj("part-000001.parquet", 4096)];
        let plan = build_resume_plan(&m, &listing);
        assert_eq!(plan.skipped(), 1);
        assert_eq!(plan.rewrites(), 1);
        assert_eq!(plan.quarantines(), 0);
        assert_eq!(
            plan.per_part["part-000002.parquet"].decision,
            ResumeDecision::Rewrite
        );
    }

    /// Row: manifest no + obj yes → quarantine (untracked).
    #[test]
    fn untracked_object_is_quarantined() {
        let m = manifest(vec![part(1, "part-000001.parquet", 4096)]);
        let listing = vec![obj("part-000001.parquet", 4096), obj("rogue.parquet", 99)];
        let plan = build_resume_plan(&m, &listing);
        assert_eq!(plan.skipped(), 1);
        assert_eq!(plan.untracked.len(), 1);
        assert_eq!(
            plan.untracked["rogue.parquet"],
            UntrackedDecision::Quarantine
        );
    }

    /// Row: manifest no + obj no → new (write).
    #[test]
    fn new_part_not_yet_committed_has_no_decision_entry() {
        let m = manifest(vec![]);
        let listing: Vec<ObjectMeta> = vec![];
        let plan = build_resume_plan(&m, &listing);
        assert!(plan.per_part.is_empty());
        assert!(plan.untracked.is_empty());
    }

    #[test]
    fn manifest_and_success_themselves_are_not_flagged_as_untracked() {
        let m = manifest(vec![part(1, "part-000001.parquet", 4096)]);
        let listing = vec![
            obj("part-000001.parquet", 4096),
            obj(MANIFEST_FILENAME, 600),
            obj(SUCCESS_FILENAME, 22),
        ];
        let plan = build_resume_plan(&m, &listing);
        assert!(
            plan.untracked.is_empty(),
            "trust-contract files must not be 'untracked'"
        );
    }

    #[test]
    fn objects_under_quarantine_prefix_are_silently_ignored() {
        let m = manifest(vec![part(1, "part-000001.parquet", 4096)]);
        let listing = vec![
            obj("part-000001.parquet", 4096),
            obj("_quarantine/r0/old.parquet", 1234),
        ];
        let plan = build_resume_plan(&m, &listing);
        assert!(plan.untracked.is_empty());
    }

    #[test]
    fn quarantined_manifest_entries_do_not_get_resume_decisions() {
        let mut quarantined_part = part(2, "old-corrupt.parquet", 999);
        quarantined_part.status = PartStatus::Quarantined;
        let m = manifest(vec![part(1, "part-000001.parquet", 4096), quarantined_part]);
        let listing = vec![obj("part-000001.parquet", 4096)];
        let plan = build_resume_plan(&m, &listing);
        assert_eq!(
            plan.per_part.len(),
            1,
            "only the committed entry gets a decision"
        );
        assert!(!plan.per_part.contains_key("old-corrupt.parquet"));
    }

    #[test]
    fn mixed_run_produces_per_decision_counts() {
        let m = manifest(vec![
            part(1, "p1.parquet", 100),
            part(2, "p2.parquet", 200),
            part(3, "p3.parquet", 300),
            part(4, "p4.parquet", 400),
        ]);
        let listing = vec![
            obj("p1.parquet", 100),
            obj("p3.parquet", 9999),
            obj("p4.parquet", 400),
            obj("rogue.parquet", 50),
        ];
        let plan = build_resume_plan(&m, &listing);
        assert_eq!(plan.skipped(), 2);
        assert_eq!(plan.rewrites(), 1);
        assert_eq!(plan.quarantines(), 1);
        assert_eq!(plan.untracked.len(), 1);
        assert_eq!(plan.per_part["p1.parquet"].decision, ResumeDecision::Skip);
        assert_eq!(
            plan.per_part["p2.parquet"].decision,
            ResumeDecision::Rewrite
        );
        assert!(matches!(
            plan.per_part["p3.parquet"].decision,
            ResumeDecision::Quarantine { .. }
        ));
        assert_eq!(plan.per_part["p4.parquet"].decision, ResumeDecision::Skip);
    }

    #[test]
    fn build_resume_plan_is_deterministic_for_same_inputs() {
        let m = manifest(vec![part(1, "p1.parquet", 100), part(2, "p2.parquet", 200)]);
        let listing = vec![obj("p1.parquet", 100), obj("p2.parquet", 9999)];
        let plan1 = build_resume_plan(&m, &listing);
        let plan2 = build_resume_plan(&m, &listing);
        assert_eq!(plan1, plan2);
    }

    #[test]
    fn build_resume_plan_is_listing_order_insensitive() {
        let m = manifest(vec![part(1, "p1.parquet", 100), part(2, "p2.parquet", 200)]);
        let listing_a = vec![obj("p1.parquet", 100), obj("p2.parquet", 200)];
        let listing_b = vec![obj("p2.parquet", 200), obj("p1.parquet", 100)];
        let plan_a = build_resume_plan(&m, &listing_a);
        let plan_b = build_resume_plan(&m, &listing_b);
        assert_eq!(plan_a, plan_b);
    }
}
