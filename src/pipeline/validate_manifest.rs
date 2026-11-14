//! **Layer: Observability**

use serde::{Deserialize, Serialize};

use crate::destination::{Destination, ObjectMeta};
use crate::error::Result;
use crate::manifest::census::{ManifestCensus, is_copy_key};
use crate::manifest::{
    MANIFEST_FILENAME, PartStatus, RunManifest, SUCCESS_FILENAME, join_key, parse_success_marker,
    success_marker_body,
};
use crate::pipeline::manifest_reconcile::{PartPresence, reconcile_manifest_against_listing};

pub(crate) use crate::manifest::MANIFEST_MAX_BYTES;

/// Same V21/CWE-400 defense for the sibling control artifact `_SUCCESS`, read by the SAME `verify_at_destination` under the SAME destination-writable threat model but previously UNCAPPED.
pub(crate) const SUCCESS_MARKER_MAX_BYTES: u64 = 4096;

/// How deep a `rivet validate` pass goes — a graded verify layer over the same checks, letting an operator trade thoroughness for latency / cost.
#[derive(clap::ValueEnum, Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ValidateDepth {
    /// Manifest read + self-consistency + `_SUCCESS` only (no prefix listing).
    Light,
    /// Light + part reconcile + untracked surplus (one `list_prefix`).
    Sample,
    /// Sample + the Form B value-checksum re-read (downloads parts; CSV: row counts only).
    #[default]
    Full,
}

impl ValidateDepth {
    /// True iff this level runs the `list_prefix` reconcile (part presence, size, checksum) and the untracked-surplus scan.
    fn runs_part_reconcile(self) -> bool {
        !matches!(self, ValidateDepth::Light)
    }

    /// True iff this level downloads part *bodies*.
    pub(crate) fn runs_part_download(self) -> bool {
        matches!(self, ValidateDepth::Full)
    }

    /// Stable operator-/wire-facing label for the depth a verdict was produced at, surfaced in `summary.json` (`depth_level`) and `rivet validate` output.
    pub fn label(self) -> &'static str {
        match self {
            ValidateDepth::Light => "light",
            ValidateDepth::Sample => "sample",
            ValidateDepth::Full => "full",
        }
    }
}

/// Read `key` into memory only if its `head()`-reported size is within `max_bytes`.
pub(crate) fn read_capped(dest: &dyn Destination, key: &str, max_bytes: u64) -> Result<Vec<u8>> {
    match dest.head(key)? {
        None => crate::rivet_bail!(
            crate::error::codes::DEST_OBJECT_NOT_FOUND,
            "'{key}' not found at the destination"
        ),
        Some(meta) => {
            if meta.size_bytes > max_bytes {
                crate::rivet_bail!(
                    crate::error::codes::DEST_CONTROL_ARTIFACT_INVALID,
                    "'{key}' is {} bytes, exceeding the {max_bytes}-byte control-artifact \
                     read cap — refusing to load it into memory (possible tampering)",
                    meta.size_bytes
                );
            }
            dest.read(key)
        }
    }
}

/// Outcome of a single `--validate` pass over a destination prefix.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestVerification {
    /// True iff a `manifest.json` was found at the destination and parsed.
    pub manifest_found: bool,
    /// Mirrors ADR-0012 M6's required `legacy_run` operator-facing label.
    pub legacy_run: bool,
    /// Manifest parts whose presence and recorded `size_bytes` were confirmed at the destination.
    pub parts_verified: usize,
    /// Subset of `parts_verified` whose **content** was confirmed via an MD5 the store surfaced in its listing (no download).
    #[serde(default)]
    pub parts_md5_verified: usize,
    /// Manifest parts that were declared `committed` but not actually present, present at a different size, or otherwise mismatched.
    pub parts_failed: usize,
    /// True iff `_SUCCESS` exists at the destination AND its body matches the fingerprint of the bytes we read for `manifest.json`.
    pub success_marker_consistent: bool,
    /// Self-consistency of the manifest (`row_count`, `part_count`, duplicate `part_id`s).
    pub manifest_self_consistent: bool,
    /// Final verdict, **derived** (not hand-maintained).
    pub passed: bool,
    /// Per-failure detail.
    pub failures: Vec<Failure>,
    /// The graded depth this verdict was produced at: `"light"`, `"sample"`, or `"full"` (see [`ValidateDepth`]).
    #[serde(default = "default_depth_level")]
    pub depth_level: String,
}

/// `serde(default)` for [`ManifestVerification::depth_level`].
fn default_depth_level() -> String {
    ValidateDepth::Full.label().to_string()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Failure {
    /// Manifest declared a part that does not exist at the destination.
    PartMissing { part_id: u32, path: String },
    /// Manifest declared a part whose actual size differs from `size_bytes`.
    PartSizeMismatch {
        part_id: u32,
        path: String,
        expected: u64,
        actual: u64,
    },
    /// Part present at the recorded size but its content MD5 (from the store's listing metadata) differs from the manifest's.
    PartChecksumMismatch {
        part_id: u32,
        path: String,
        expected: String,
        actual: String,
    },
    /// `--depth full` re-read the parts and a per-column VALUE checksum (Form B) disagreed with the manifest.
    ValueChecksumMismatch { detail: String },
    /// `--depth full` found a part whose DECLARED row count disagrees with the rows the file holds.
    PartRowCountMismatch { detail: String },
    /// `--depth full` re-read a CDC export's parts and the `__pos` continuity check found a gap/duplicate in the change positions.
    CdcPositionViolation { detail: String },
    /// `_SUCCESS` exists but its body is malformed (not `xxh3:<16-hex>` after trim).
    SuccessMarkerMalformed { body_preview: String },
    /// `_SUCCESS` body parsed but does not match `xxh3(manifest.json bytes)`.
    SuccessMarkerStale {
        marker_fingerprint: String,
        manifest_fingerprint: String,
    },
    /// `RunManifest::validate_self_consistency` rejected the manifest.
    ManifestSelfInconsistent { detail: String },
    /// Reading `manifest.json` returned an I/O error other than "absent".
    ManifestReadError { detail: String },
    /// Reading `_SUCCESS` returned an I/O error other than "absent".
    SuccessMarkerReadError { detail: String },
    /// Listing the destination prefix returned an I/O error.
    ListPrefixError { detail: String },
    /// A file is present at the destination prefix but no manifest entry references it.
    UntrackedObject { key: String, size_bytes: u64 },
    /// The export declared `verify: content` but some parts could only be size-verified (no comparable content checksum from the store).
    ContentVerificationUnmet { size_only: usize, total: usize },
    /// A manifest was *required* at this prefix (the operator pinned a literal `--prefix`, asserting a real dataset lives here) but none was found.
    ManifestRequiredButAbsent { prefix: String },
    /// A `partition_by` partition the source holds rows for has no manifest at its prefix.
    PartitionNotDelivered { prefix: String, export: String },
    /// The state records a successful run of `export` at this prefix and the destination holds no manifest and no `_SUCCESS` there.
    RecordedDeliveryAbsent { prefix: String, export: String },
    /// `_SUCCESS` is present and the manifest it was written after is not.
    ManifestMissingUnderMarker { prefix: String },
    /// The manifest's own run status is not `success` (`failed`, `interrupted`, `running`).
    RunNotSuccessful { status: String },
    /// Advisory: `--depth full` re-counted this CSV export's rows per part.
    ValueCheckNotAvailable { format: String },
}

impl Failure {
    /// Whether this failure invalidates the dataset (flips `passed` to false).
    pub fn is_fatal(&self) -> bool {
        !matches!(
            self,
            Failure::UntrackedObject { .. } | Failure::ValueCheckNotAvailable { .. }
        )
    }

    /// Whether this failure is COULD-NOT-VERIFY (an operational I/O error that prevented the check) rather than VERIFIED-WRONG (the check ran and found the data bad).
    pub fn is_could_not_verify(&self) -> bool {
        matches!(
            self,
            Failure::ManifestReadError { .. }
                | Failure::SuccessMarkerReadError { .. }
                | Failure::ListPrefixError { .. }
                | Failure::RunNotSuccessful { .. }
        )
    }

    /// Stable `RIVET_VERIFY_*` error code for this failure variant.
    pub fn error_code(&self) -> &'static str {
        match self {
            Failure::PartMissing { .. } => "RIVET_VERIFY_PART_MISSING",
            Failure::PartSizeMismatch { .. } => "RIVET_VERIFY_PART_SIZE_MISMATCH",
            Failure::PartChecksumMismatch { .. } => "RIVET_VERIFY_PART_CHECKSUM_MISMATCH",
            Failure::ValueChecksumMismatch { .. } => "RIVET_VERIFY_VALUE_CHECKSUM",
            Failure::PartRowCountMismatch { .. } => "RIVET_VERIFY_PART_ROW_COUNT",
            Failure::CdcPositionViolation { .. } => "RIVET_VERIFY_CDC_POSITION",
            Failure::SuccessMarkerMalformed { .. } => "RIVET_VERIFY_SUCCESS_MALFORMED",
            Failure::SuccessMarkerStale { .. } => "RIVET_VERIFY_SUCCESS_STALE",
            Failure::ManifestSelfInconsistent { .. } => "RIVET_VERIFY_MANIFEST_INCONSISTENT",
            Failure::ManifestReadError { .. } => "RIVET_VERIFY_MANIFEST_READ_ERROR",
            Failure::SuccessMarkerReadError { .. } => "RIVET_VERIFY_SUCCESS_READ_ERROR",
            Failure::ListPrefixError { .. } => "RIVET_VERIFY_LIST_ERROR",
            Failure::UntrackedObject { .. } => "RIVET_VERIFY_UNTRACKED_OBJECT",
            Failure::ContentVerificationUnmet { .. } => "RIVET_VERIFY_CONTENT_UNMET",
            Failure::ManifestRequiredButAbsent { .. } => "RIVET_VERIFY_MANIFEST_REQUIRED",
            Failure::PartitionNotDelivered { .. } => "RIVET_VERIFY_PARTITION_NOT_DELIVERED",
            Failure::RecordedDeliveryAbsent { .. } => "RIVET_VERIFY_DELIVERY_ABSENT",
            Failure::ManifestMissingUnderMarker { .. } => "RIVET_VERIFY_MANIFEST_MISSING",
            Failure::RunNotSuccessful { .. } => "RIVET_VERIFY_RUN_NOT_SUCCESSFUL",
            Failure::ValueCheckNotAvailable { .. } => "RIVET_VERIFY_VALUE_CHECK_NOT_AVAILABLE",
        }
    }
}

impl std::fmt::Display for Failure {
    /// One operator-facing line per failure variant.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Failure::PartMissing { part_id, path } => {
                write!(f, "part {} missing at {}", part_id, path)
            }
            Failure::PartSizeMismatch {
                part_id,
                path,
                expected,
                actual,
            } => write!(
                f,
                "part {} size mismatch at {}: manifest {}, dest {}",
                part_id, path, expected, actual
            ),
            Failure::PartRowCountMismatch { detail } => {
                write!(f, "part row count disagrees with the part: {}", detail)
            }
            Failure::ValueChecksumMismatch { detail } => {
                write!(
                    f,
                    "value checksum mismatch (post-write corruption): {}",
                    detail
                )
            }
            Failure::CdcPositionViolation { detail } => {
                write!(f, "cdc __pos continuity violation: {}", detail)
            }
            Failure::PartChecksumMismatch {
                part_id,
                path,
                expected,
                actual,
            } => write!(
                f,
                "part {} content mismatch at {}: manifest md5 {}, dest {}",
                part_id, path, expected, actual
            ),
            Failure::SuccessMarkerMalformed { body_preview } => {
                write!(f, "_SUCCESS body malformed: {body_preview:?}")
            }
            Failure::SuccessMarkerStale {
                marker_fingerprint,
                manifest_fingerprint,
            } => write!(
                f,
                "_SUCCESS body {} != manifest fingerprint {} (stale marker)",
                marker_fingerprint, manifest_fingerprint
            ),
            Failure::ManifestSelfInconsistent { detail } => {
                write!(f, "manifest self-consistency: {detail}")
            }
            Failure::ManifestReadError { detail } => {
                write!(f, "manifest read error: {detail}")
            }
            Failure::SuccessMarkerReadError { detail } => {
                write!(f, "_SUCCESS read error: {detail}")
            }
            Failure::ListPrefixError { detail } => {
                write!(f, "destination listing error: {detail}")
            }
            Failure::UntrackedObject { key, size_bytes } => {
                write!(f, "untracked object: {} ({} bytes)", key, size_bytes)
            }
            Failure::ContentVerificationUnmet { size_only, total } => write!(
                f,
                "verify: content not met — {size_only} of {total} part(s) only \
                 size-verified (no store checksum); lower max_file_size so parts \
                 upload as a single PUT, or the backend exposes no checksum"
            ),
            Failure::ManifestRequiredButAbsent { prefix } => write!(
                f,
                "no manifest at {prefix}: a manifest was required here (operator \
                 pinned --prefix) but none was found — this prefix was never \
                 written, or the data was relocated. Run the export first, or \
                 drop --prefix to validate the config-resolved destination."
            ),
            Failure::PartitionNotDelivered { prefix, export } => write!(
                f,
                "no manifest at {prefix}: the source holds this partition of `{export}` but the \
                 destination holds none of it — it was never delivered or its directory was \
                 removed. Re-deliver it with `rivet run --export {export}`"
            ),
            Failure::RecordedDeliveryAbsent { prefix, export } => write!(
                f,
                "no manifest at {prefix}: the state records a successful run of `{export}` here and \
                 the destination holds none of it — the delivery was removed or the destination \
                 moved. Restore the prefix from a backup, or deliver it again with `rivet run \
                 --export {export}` (for an incremental or chunked export run `rivet state reset \
                 --export {export}` first, so the run starts over instead of continuing from its \
                 stored progress)"
            ),
            Failure::ManifestMissingUnderMarker { prefix } => write!(
                f,
                "`_SUCCESS` is present at {prefix} and manifest.json is not: the marker is written \
                 only after the manifest, so the manifest was removed and the parts here cannot be \
                 verified. Restore manifest.json, or run the export again to write the prefix anew."
            ),
            Failure::RunNotSuccessful { status } => write!(
                f,
                "the manifest records its last run as {status}, not success: this prefix \
                 does not hold a completed export. Re-run the export."
            ),
            Failure::ValueCheckNotAvailable { format } => write!(
                f,
                "cell values were not re-read: {format} parts carry no value checksum \
                 (only parquet does); each part's row count was re-counted instead"
            ),
        }
    }
}

impl ManifestVerification {
    /// Base verdict: nothing checked yet (no manifest, all counts zero, all sub-checks false, `passed = false`).
    fn empty() -> Self {
        Self {
            manifest_found: false,
            legacy_run: false,
            parts_verified: 0,
            parts_md5_verified: 0,
            parts_failed: 0,
            success_marker_consistent: false,
            manifest_self_consistent: false,
            passed: false,
            failures: Vec::new(),
            depth_level: default_depth_level(),
        }
    }

    /// Recompute `passed` from the verdict's facts.
    fn recompute_passed(&mut self) {
        self.passed = self.manifest_found && !self.failures.iter().any(Failure::is_fatal);
    }

    /// Apply the export's `verify` policy (ADR-0013 / review D).
    pub fn enforce_content_policy(&mut self, require_content: bool) {
        if require_content && self.manifest_found {
            let size_only = self.parts_verified.saturating_sub(self.parts_md5_verified);
            if size_only > 0 {
                self.failures.push(Failure::ContentVerificationUnmet {
                    size_only,
                    total: self.parts_verified,
                });
                self.recompute_passed();
            }
        }
    }

    /// Apply the "a manifest must exist here" policy (finding #20).
    pub fn require_manifest_present(&mut self, prefix: &str) {
        self.require_manifest_with(Failure::ManifestRequiredButAbsent {
            prefix: prefix.to_string(),
        });
    }

    /// Turn the same bare absent-manifest verdict into the fatal `failure` of a partition the source holds.
    pub fn require_partition_delivered(&mut self, prefix: &str, export: &str) {
        self.require_manifest_with(Failure::PartitionNotDelivered {
            prefix: prefix.to_string(),
            export: export.to_string(),
        });
    }

    /// Turn the same bare absent-manifest verdict into the fatal `failure` of a delivery the state records.
    pub fn require_recorded_delivery(&mut self, prefix: &str, export: &str) {
        self.require_manifest_with(Failure::RecordedDeliveryAbsent {
            prefix: prefix.to_string(),
            export: export.to_string(),
        });
    }

    /// Whether this is the bare absent-manifest verdict: no manifest, no `_SUCCESS`, no other failure.
    pub fn is_bare_legacy(&self) -> bool {
        self.legacy_run && !self.manifest_found && !self.has_failures()
    }

    /// Push `failure` for an absent manifest that carries no other failure.
    fn require_manifest_with(&mut self, failure: Failure) {
        if !self.manifest_found && !self.has_failures() {
            self.legacy_run = false;
            self.failures.push(failure);
            self.recompute_passed();
        }
    }

    /// Construct the M6 (legacy run) verdict for a destination that has no manifest at all.
    pub fn legacy() -> Self {
        Self {
            legacy_run: true,
            ..Self::empty()
        }
    }

    /// True iff this verification surfaced any explicit failure (i.e.
    pub fn has_failures(&self) -> bool {
        !self.failures.is_empty()
    }

    /// Whether any recorded failure is VERIFIED-WRONG (the check ran and the data is bad) as opposed to purely COULD-NOT-VERIFY (an I/O read/list error).
    pub fn has_verified_wrong_failure(&self) -> bool {
        self.failures.iter().any(|f| !f.is_could_not_verify())
    }
}

/// Run the manifest-aware verification at `manifest_dir` (the destination- relative directory containing `manifest.json` and `_SUCCESS`).
fn read_sibling_manifests(
    dest: &dyn Destination,
    listing: &[ObjectMeta],
) -> Vec<(String, RunManifest)> {
    let mut out = Vec::new();
    for meta in listing {
        if !is_copy_key(&meta.key) {
            continue;
        }
        if let Ok(bytes) = read_capped(dest, &meta.key, MANIFEST_MAX_BYTES)
            && let Ok(m) = serde_json::from_slice::<RunManifest>(&bytes)
        {
            out.push((meta.key.clone(), m));
        }
    }
    out
}

/// A working manifest whose `parts` are the UNION of `canonical`'s parts and every `Committed` part declared by a SAME-FAMILY **split unit** under the prefix, deduped by declared path.
fn merge_split_unit_parts(
    canonical: &RunManifest,
    census: &ManifestCensus<'_>,
    manifest_dir: &str,
) -> RunManifest {
    let mut merged = canonical.clone();
    let mut seen: std::collections::BTreeSet<String> =
        merged.parts.iter().map(|p| p.path.clone()).collect();
    for unit in census.completed_split_units(&canonical.export_family) {
        for p in &unit.manifest.parts {
            if p.status == PartStatus::Committed && seen.insert(p.path.clone()) {
                merged.parts.push(p.clone());
            }
        }
    }
    if (merged.parts.is_empty() || canonical.mode == "cdc") && !canonical.export_family.is_empty() {
        for run in census.runs() {
            if run.family() != canonical.export_family {
                continue;
            }
            if run.manifest.status != crate::manifest::ManifestStatus::Success {
                continue;
            }
            let rel = run
                .key
                .strip_prefix(manifest_dir)
                .unwrap_or(run.key)
                .trim_start_matches('/');
            if rel.contains('/') {
                continue;
            }
            for p in &run.manifest.parts {
                if p.status == PartStatus::Committed && seen.insert(p.path.clone()) {
                    merged.parts.push(p.clone());
                }
            }
        }
    }
    merged
}

pub fn verify_at_destination(
    dest: &dyn Destination,
    manifest_dir: &str,
    depth: ValidateDepth,
) -> Result<ManifestVerification> {
    let manifest_key = join_key(manifest_dir, MANIFEST_FILENAME);
    let success_key = join_key(manifest_dir, SUCCESS_FILENAME);

    let with_depth = |mut v: ManifestVerification| -> ManifestVerification {
        v.depth_level = depth.label().to_string();
        v
    };

    let manifest_bytes = match dest.head(&manifest_key) {
        Ok(None) => {
            let mut v = ManifestVerification::legacy();
            let orphan_marker = match dest.head(&success_key) {
                Ok(None) => None,
                Ok(Some(_)) => Some(Failure::ManifestMissingUnderMarker {
                    prefix: manifest_dir.to_string(),
                }),
                Err(e) => Some(Failure::SuccessMarkerReadError {
                    detail: format!("_SUCCESS head failed: {e:#}"),
                }),
            };
            if let Some(failure) = orphan_marker {
                v.legacy_run = false;
                v.failures.push(failure);
                v.recompute_passed();
            }
            return Ok(with_depth(v));
        }
        Ok(Some(_)) => match read_capped(dest, &manifest_key, MANIFEST_MAX_BYTES) {
            Ok(b) => b,
            Err(e) => {
                let mut v = ManifestVerification::legacy();
                v.legacy_run = false;
                v.failures.push(Failure::ManifestReadError {
                    detail: format!("{e:#}"),
                });
                v.passed = false;
                return Ok(with_depth(v));
            }
        },
        Err(e) => {
            let mut v = ManifestVerification::legacy();
            v.legacy_run = false;
            v.failures.push(Failure::ManifestReadError {
                detail: format!("manifest head failed: {e:#}"),
            });
            v.passed = false;
            return Ok(with_depth(v));
        }
    };

    let manifest: RunManifest = match serde_json::from_slice(&manifest_bytes) {
        Ok(m) => m,
        Err(e) => {
            return Ok(with_depth(ManifestVerification {
                manifest_found: true,
                failures: vec![Failure::ManifestSelfInconsistent {
                    detail: format!("manifest.json parse failed: {e}"),
                }],
                ..ManifestVerification::empty()
            }));
        }
    };

    let mut out = ManifestVerification {
        manifest_found: true,
        manifest_self_consistent: true,
        passed: true,
        depth_level: depth.label().to_string(),
        ..ManifestVerification::empty()
    };

    if let Err(e) = manifest.validate_self_consistency() {
        out.manifest_self_consistent = false;
        out.failures.push(Failure::ManifestSelfInconsistent {
            detail: format!("{e}"),
        });
    }
    if manifest.status != crate::manifest::ManifestStatus::Success {
        let status = serde_json::to_value(manifest.status)?;
        out.failures.push(Failure::RunNotSuccessful {
            status: status.as_str().unwrap_or_default().to_string(),
        });
    }

    let reconciliation = if depth.runs_part_reconcile() {
        match dest.list_prefix(manifest_dir) {
            Ok(listing) => {
                let siblings = if manifest.export_family.is_empty() {
                    Vec::new()
                } else {
                    read_sibling_manifests(dest, &listing)
                };
                let census = ManifestCensus::new(&siblings);
                let target = merge_split_unit_parts(&manifest, &census, manifest_dir);
                let mut rec = reconcile_manifest_against_listing(&target, &listing, manifest_dir);
                if !rec.untracked.is_empty() && !manifest.export_family.is_empty() {
                    let claimed = census.claimed_parts(&manifest.export_family, manifest_dir);
                    rec.untracked.retain(|o| !claimed.contains(&o.key));
                }
                Some(rec)
            }
            Err(e) => {
                out.failures.push(Failure::ListPrefixError {
                    detail: format!("{e:#}"),
                });
                None
            }
        }
    } else {
        None
    };
    if let Some(rec) = &reconciliation {
        for check in &rec.per_part {
            match &check.presence {
                PartPresence::Present { md5_verified } => {
                    out.parts_verified += 1;
                    if *md5_verified {
                        out.parts_md5_verified += 1;
                    }
                }
                PartPresence::SizeMismatch { expected, actual } => {
                    out.parts_failed += 1;
                    out.failures.push(Failure::PartSizeMismatch {
                        part_id: check.part_id,
                        path: check.path.clone(),
                        expected: *expected,
                        actual: *actual,
                    });
                }
                PartPresence::Missing => {
                    out.parts_failed += 1;
                    out.failures.push(Failure::PartMissing {
                        part_id: check.part_id,
                        path: check.path.clone(),
                    });
                }
                PartPresence::ChecksumMismatch { expected, actual } => {
                    out.parts_failed += 1;
                    out.failures.push(Failure::PartChecksumMismatch {
                        part_id: check.part_id,
                        path: check.path.clone(),
                        expected: expected.clone(),
                        actual: actual.clone(),
                    });
                }
            }
        }
    }

    let success_head = match dest.head(&success_key) {
        Ok(h) => h,
        Err(e) => {
            out.failures.push(Failure::SuccessMarkerReadError {
                detail: format!("_SUCCESS head failed: {e:#}"),
            });
            out.recompute_passed();
            return Ok(out);
        }
    };
    match success_head {
        None => {}
        Some(head) if head.size_bytes > SUCCESS_MARKER_MAX_BYTES => {
            out.failures.push(Failure::SuccessMarkerMalformed {
                body_preview: format!(
                    "(oversized: {} bytes exceeds the {SUCCESS_MARKER_MAX_BYTES}-byte _SUCCESS cap; not read)",
                    head.size_bytes
                ),
            });
            out.recompute_passed();
            return Ok(out);
        }
        Some(_) => match dest.read(&success_key) {
            Err(e) => {
                out.failures.push(Failure::SuccessMarkerReadError {
                    detail: format!("{e:#}"),
                });
            }
            Ok(body) => {
                let body_str = match std::str::from_utf8(&body) {
                    Ok(s) => s,
                    Err(_) => {
                        out.failures.push(Failure::SuccessMarkerMalformed {
                            body_preview: format!("(non-utf8, {} bytes)", body.len()),
                        });
                        out.recompute_passed();
                        return Ok(out);
                    }
                };
                match parse_success_marker(body_str) {
                    None => {
                        out.failures.push(Failure::SuccessMarkerMalformed {
                            body_preview: preview(body_str),
                        });
                    }
                    Some(marker_fp) => {
                        let manifest_fp = success_marker_body(&manifest_bytes);
                        let manifest_fp_trimmed = manifest_fp.trim_end_matches('\n');
                        if marker_fp == manifest_fp_trimmed {
                            out.success_marker_consistent = true;
                        } else {
                            out.failures.push(Failure::SuccessMarkerStale {
                                marker_fingerprint: marker_fp.to_string(),
                                manifest_fingerprint: manifest_fp_trimmed.to_string(),
                            });
                        }
                    }
                }
            }
        },
    }

    if let Some(rec) = reconciliation {
        for obj in rec.untracked {
            out.failures.push(Failure::UntrackedObject {
                key: obj.key,
                size_bytes: obj.size_bytes,
            });
        }
    }

    out.recompute_passed();
    Ok(out)
}

/// Truncate `s` to a small printable preview for error messages.
fn preview(s: &str) -> String {
    let trimmed: String = s.chars().take(40).collect();
    if s.chars().count() > 40 {
        format!("{trimmed}…")
    } else {
        trimmed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{DestinationConfig, DestinationType};
    use crate::destination::local::LocalDestination;
    use crate::manifest::{
        MANIFEST_VERSION, ManifestDestination, ManifestPart, ManifestSource, ManifestStatus,
        PartStatus, RunManifest,
    };
    use std::path::Path;

    /// Only no manifest, a legacy label and no failure together make the bare absent-manifest verdict.
    #[test]
    fn only_no_manifest_a_legacy_label_and_no_failure_is_bare_legacy() {
        assert!(ManifestVerification::legacy().is_bare_legacy());
        let not_legacy = ManifestVerification {
            legacy_run: false,
            ..ManifestVerification::legacy()
        };
        assert!(!not_legacy.is_bare_legacy());
        let found = ManifestVerification {
            manifest_found: true,
            ..ManifestVerification::legacy()
        };
        assert!(!found.is_bare_legacy());
        let failed = ManifestVerification {
            failures: vec![Failure::ManifestReadError {
                detail: "denied".into(),
            }],
            ..ManifestVerification::legacy()
        };
        assert!(!failed.is_bare_legacy());
    }

    fn local_dest(base: &Path) -> LocalDestination {
        LocalDestination::new(&DestinationConfig {
            destination_type: DestinationType::Local,
            path: Some(base.to_string_lossy().into_owned()),
            ..Default::default()
        })
        .unwrap()
    }

    fn part(part_id: u32, rows: i64, size: u64, fp: &str) -> ManifestPart {
        ManifestPart {
            part_id,
            path: format!("part-{part_id:06}.parquet"),
            rows,
            size_bytes: size,
            content_fingerprint: fp.into(),
            content_md5: String::new(),
            status: PartStatus::Committed,
        }
    }

    fn build_manifest(parts: Vec<ManifestPart>, status: ManifestStatus) -> RunManifest {
        let row_count: i64 = parts
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
            run_id: "r".into(),
            export_name: "public.orders".into(),
            export_family: String::new(),
            started_at: "2026-05-21T12:00:00Z".into(),
            finished_at: "2026-05-21T12:01:00Z".into(),
            status,
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
            schema_fingerprint: "xxh3:0123456789abcdef".into(),
            row_count,
            part_count,
            parts,
            column_checksums: None,
            checksum_key_column: None,
        }
    }

    /// Lay out a clean dataset with manifest + _SUCCESS at the root.
    fn write_dataset(dir: &Path, m: &RunManifest, parts_with_bytes: &[(&str, &[u8])]) {
        for (name, bytes) in parts_with_bytes {
            std::fs::write(dir.join(name), bytes).unwrap();
        }
        let body = serde_json::to_vec_pretty(m).unwrap();
        std::fs::write(dir.join(MANIFEST_FILENAME), &body).unwrap();
        if matches!(m.status, ManifestStatus::Success) {
            std::fs::write(dir.join(SUCCESS_FILENAME), success_marker_body(&body)).unwrap();
        }
    }

    fn split_win() -> crate::manifest::SplitWindow {
        crate::manifest::SplitWindow {
            key_column: "id".into(),
            lo: None,
            hi: Some("1000".into()),
        }
    }

    /// A sibling copy keyed the way the prefix listing hands it to the census.
    fn sib_keyed(m: RunManifest) -> (String, RunManifest) {
        (crate::manifest::run_unique_manifest_name(&m.export_name), m)
    }

    /// merge_split_unit_parts folds every SAME-FAMILY SPLIT-UNIT sibling's committed parts into the reconcile target, and EXCLUDES both a FOREIGN family's parts and a SAME-FAMILY non-split (plain, superseded) copy's parts.
    #[test]
    fn a_zero_part_canonical_folds_its_family_and_nothing_else() {
        let unit = |name: &str, path: &str, fam: &str, st: PartStatus| {
            let mut m = build_manifest(vec![part(0, 10, 100, "fp")], ManifestStatus::Success);
            m.export_family = fam.into();
            m.export_name = name.into();
            m.parts[0].path = path.into();
            m.parts[0].status = st;
            sib_keyed(m)
        };
        let mut canonical = build_manifest(vec![], ManifestStatus::Success);
        canonical.export_family = "daily".into();
        canonical.export_name = "daily".into();

        let siblings = vec![
            unit("daily", "mine_ok.parquet", "daily", PartStatus::Committed),
            unit(
                "daily",
                "mine_pending.parquet",
                "daily",
                PartStatus::Quarantined,
            ),
            unit("other", "foreign.parquet", "other", PartStatus::Committed),
        ];
        let merged = merge_split_unit_parts(&canonical, &ManifestCensus::new(&siblings), "");
        let paths: std::collections::BTreeSet<&str> =
            merged.parts.iter().map(|p| p.path.as_str()).collect();
        assert_eq!(
            paths,
            ["mine_ok.parquet"].into_iter().collect(),
            "only the same-family COMMITTED part. A foreign one means the family \
             check inverted; a quarantined one means the status test did; nothing at \
             all means the zero-parts fold never ran: {paths:?}"
        );

        let mut familyless = canonical.clone();
        familyless.export_family = String::new();
        assert!(
            merge_split_unit_parts(&familyless, &ManifestCensus::new(&siblings), "")
                .parts
                .is_empty(),
            "a family-less canonical folds nothing — dropping the `!` here would \
             invert exactly that"
        );
    }

    #[test]
    fn merge_split_unit_parts_folds_split_siblings_only() {
        let unit = |name: &str, path: &str, fam: &str, split: bool| {
            let mut m = build_manifest(vec![part(0, 10, 100, "fp")], ManifestStatus::Success);
            m.export_family = fam.into();
            m.export_name = name.into();
            m.parts[0].path = path.into();
            m.split_window = if split { Some(split_win()) } else { None };
            sib_keyed(m)
        };
        let canonical = unit("daily#3", "daily#3_p.parquet", "daily", true).1;
        let siblings = vec![
            unit("daily#0", "daily#0_p.parquet", "daily", true),
            unit("daily#1", "daily#1_p.parquet", "daily", true),
            unit("daily#3", "daily#3_p.parquet", "daily", true),
            unit("daily", "daily_old.parquet", "daily", false),
            unit("other#0", "other_p.parquet", "other", true),
        ];
        let merged = merge_split_unit_parts(&canonical, &ManifestCensus::new(&siblings), "");
        let paths: std::collections::BTreeSet<&str> =
            merged.parts.iter().map(|p| p.path.as_str()).collect();
        assert_eq!(
            paths,
            [
                "daily#0_p.parquet",
                "daily#1_p.parquet",
                "daily#3_p.parquet"
            ]
            .into_iter()
            .collect(),
            "fold same-family SPLIT units only — never a foreign family, never a same-family \
             plain/superseded copy: {paths:?}"
        );
        assert_eq!(
            merged
                .parts
                .iter()
                .filter(|p| p.path == "daily#3_p.parquet")
                .count(),
            1,
            "the canonical's own sibling copy must not double its part"
        );
    }

    /// A non-committed (quarantined) split-unit sibling part is NOT merged.
    #[test]
    fn merge_split_unit_parts_skips_non_committed_siblings() {
        let mut canonical = build_manifest(vec![part(0, 10, 100, "fp")], ManifestStatus::Success);
        canonical.export_family = "daily".into();
        canonical.export_name = "daily#0".into();
        canonical.parts[0].path = "daily#0_p.parquet".into();
        canonical.split_window = Some(split_win());

        let mut sib = build_manifest(vec![part(0, 10, 100, "fp")], ManifestStatus::Success);
        sib.export_family = "daily".into();
        sib.export_name = "daily#1".into();
        sib.parts[0].path = "daily#1_q.parquet".into();
        sib.parts[0].status = PartStatus::Quarantined;
        sib.split_window = Some(split_win());

        let siblings = vec![sib_keyed(sib)];
        let merged = merge_split_unit_parts(&canonical, &ManifestCensus::new(&siblings), "");
        let paths: Vec<&str> = merged.parts.iter().map(|p| p.path.as_str()).collect();
        assert_eq!(
            paths,
            vec!["daily#0_p.parquet"],
            "a quarantined split-unit part must not be folded into the reconcile target"
        );
    }

    /// End-to-end: a `--pool --split` prefix where a NON-last unit's part is missing must FAIL validation.
    #[test]
    fn verify_over_a_split_prefix_catches_a_missing_non_last_unit_part() {
        use crate::manifest::run_unique_manifest_name;
        let dir = tempfile::tempdir().unwrap();

        let mut u0 = build_manifest(
            vec![part(0, 10, 4, "xxh3:0000000000000000")],
            ManifestStatus::Success,
        );
        u0.export_family = "orders".into();
        u0.export_name = "orders#0".into();
        u0.run_id = "r0".into();
        u0.parts[0].path = "orders#0-part.parquet".into();
        u0.split_window = Some(crate::manifest::SplitWindow {
            key_column: "id".into(),
            lo: None,
            hi: Some("1000".into()),
        });

        let mut u1 = build_manifest(
            vec![part(1, 20, 5, "xxh3:1111111111111111")],
            ManifestStatus::Success,
        );
        u1.export_family = "orders".into();
        u1.export_name = "orders#1".into();
        u1.run_id = "r1".into();
        u1.parts[0].path = "orders#1-part.parquet".into();
        u1.split_window = Some(crate::manifest::SplitWindow {
            key_column: "id".into(),
            lo: Some("1000".into()),
            hi: None,
        });

        std::fs::write(
            dir.path().join(run_unique_manifest_name("r0")),
            serde_json::to_vec(&u0).unwrap(),
        )
        .unwrap();
        let u1_body = serde_json::to_vec(&u1).unwrap();
        std::fs::write(dir.path().join(run_unique_manifest_name("r1")), &u1_body).unwrap();
        std::fs::write(dir.path().join(MANIFEST_FILENAME), &u1_body).unwrap();
        std::fs::write(
            dir.path().join(SUCCESS_FILENAME),
            success_marker_body(&u1_body),
        )
        .unwrap();
        std::fs::write(dir.path().join("orders#1-part.parquet"), b"BBBBB").unwrap();

        let dest = local_dest(dir.path());
        let v = verify_at_destination(&dest, "", ValidateDepth::Full).unwrap();
        assert!(
            !v.passed,
            "a missing part of a NON-last split unit must fail validation, not pass silently"
        );
        assert!(
            v.failures
                .iter()
                .any(|f| matches!(f, Failure::PartMissing { .. })),
            "expected a PartMissing for the dropped orders#0 part; got {:?}",
            v.failures
        );
    }

    #[test]
    fn happy_path_verifies_all_parts_and_success_marker() {
        let dir = tempfile::tempdir().unwrap();
        let m = build_manifest(
            vec![
                part(1, 10, 4, "xxh3:1111111111111111"),
                part(2, 20, 5, "xxh3:2222222222222222"),
            ],
            ManifestStatus::Success,
        );
        write_dataset(
            dir.path(),
            &m,
            &[
                ("part-000001.parquet", b"AAAA"),
                ("part-000002.parquet", b"BBBBB"),
            ],
        );
        let dest = local_dest(dir.path());

        let v = verify_at_destination(&dest, "", ValidateDepth::Full).unwrap();
        assert!(v.manifest_found);
        assert!(!v.legacy_run);
        assert_eq!(v.parts_verified, 2);
        assert_eq!(v.parts_failed, 0);
        assert!(v.success_marker_consistent);
        assert!(v.manifest_self_consistent);
        assert!(v.passed);
        assert!(v.failures.is_empty());
    }

    /// A marker with no manifest is a prefix that lost its manifest.
    #[test]
    fn a_success_marker_without_a_manifest_is_not_a_legacy_run() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("_SUCCESS"), b"{}").unwrap();
        std::fs::write(dir.path().join("part-0.parquet"), b"rows").unwrap();
        let dest = local_dest(dir.path());
        let v = verify_at_destination(&dest, "", ValidateDepth::Full).unwrap();
        assert!(!v.legacy_run && !v.passed && !v.manifest_found);
        assert_eq!(
            v.failures,
            [Failure::ManifestMissingUnderMarker { prefix: "".into() }]
        );
        assert!(v.has_verified_wrong_failure(), "exit class: integrity");
        let mut pinned = v.clone();
        pinned.require_manifest_present("p/");
        assert_eq!(pinned.failures, v.failures, "one failure, not two");

        std::fs::remove_file(dir.path().join("_SUCCESS")).unwrap();
        let v = verify_at_destination(&dest, "", ValidateDepth::Full).unwrap();
        assert!(v.legacy_run && v.failures.is_empty());
    }

    #[test]
    fn no_manifest_returns_legacy_run_label() {
        let dir = tempfile::tempdir().unwrap();
        let dest = local_dest(dir.path());
        let v = verify_at_destination(&dest, "", ValidateDepth::Full).unwrap();
        assert!(!v.manifest_found);
        assert!(v.legacy_run);
        assert_eq!(v.parts_verified, 0);
        assert!(!v.passed);
        assert!(v.failures.is_empty(), "no failures, just a legacy label");
    }

    #[test]
    fn missing_part_is_flagged_with_part_id_and_path() {
        let dir = tempfile::tempdir().unwrap();
        let m = build_manifest(
            vec![
                part(1, 10, 4, "xxh3:1111111111111111"),
                part(2, 20, 5, "xxh3:2222222222222222"),
            ],
            ManifestStatus::Success,
        );
        write_dataset(dir.path(), &m, &[("part-000001.parquet", b"AAAA")]);
        let dest = local_dest(dir.path());

        let v = verify_at_destination(&dest, "", ValidateDepth::Full).unwrap();
        assert_eq!(v.parts_verified, 1);
        assert_eq!(v.parts_failed, 1);
        assert!(!v.passed);
        assert!(
            v.failures
                .iter()
                .any(|f| matches!(f, Failure::PartMissing { part_id: 2, .. }))
        );
    }

    #[test]
    fn part_size_mismatch_is_flagged_with_expected_and_actual() {
        let dir = tempfile::tempdir().unwrap();
        let m = build_manifest(
            vec![part(1, 10, 4, "xxh3:1111111111111111")],
            ManifestStatus::Success,
        );
        write_dataset(dir.path(), &m, &[("part-000001.parquet", b"OOPSIE")]);
        let dest = local_dest(dir.path());

        let v = verify_at_destination(&dest, "", ValidateDepth::Full).unwrap();
        assert!(!v.passed);
        let mismatch = v
            .failures
            .iter()
            .find_map(|f| match f {
                Failure::PartSizeMismatch {
                    part_id,
                    expected,
                    actual,
                    ..
                } => Some((*part_id, *expected, *actual)),
                _ => None,
            })
            .expect("must surface the size mismatch");
        assert_eq!(mismatch, (1, 4, 6));
    }

    #[test]
    fn stale_success_marker_is_flagged_as_inconsistent() {
        let dir = tempfile::tempdir().unwrap();
        let m = build_manifest(
            vec![part(1, 10, 4, "xxh3:1111111111111111")],
            ManifestStatus::Success,
        );
        write_dataset(dir.path(), &m, &[("part-000001.parquet", b"AAAA")]);
        std::fs::write(
            dir.path().join(SUCCESS_FILENAME),
            success_marker_body(b"different manifest body"),
        )
        .unwrap();
        let dest = local_dest(dir.path());

        let v = verify_at_destination(&dest, "", ValidateDepth::Full).unwrap();
        assert!(!v.success_marker_consistent);
        assert!(!v.passed);
        assert!(
            v.failures
                .iter()
                .any(|f| matches!(f, Failure::SuccessMarkerStale { .. }))
        );
    }

    #[test]
    fn malformed_success_marker_body_is_flagged() {
        let dir = tempfile::tempdir().unwrap();
        let m = build_manifest(
            vec![part(1, 10, 4, "xxh3:1111111111111111")],
            ManifestStatus::Success,
        );
        write_dataset(dir.path(), &m, &[("part-000001.parquet", b"AAAA")]);
        std::fs::write(dir.path().join(SUCCESS_FILENAME), b"not even xxh3 shaped").unwrap();
        let dest = local_dest(dir.path());

        let v = verify_at_destination(&dest, "", ValidateDepth::Full).unwrap();
        assert!(!v.passed);
        assert!(
            v.failures
                .iter()
                .any(|f| matches!(f, Failure::SuccessMarkerMalformed { .. }))
        );
    }

    #[test]
    fn absent_success_marker_does_not_fail_validation_alone() {
        let dir = tempfile::tempdir().unwrap();
        let m = build_manifest(
            vec![part(1, 10, 4, "xxh3:1111111111111111")],
            ManifestStatus::Success,
        );
        write_dataset(dir.path(), &m, &[("part-000001.parquet", b"AAAA")]);
        std::fs::remove_file(dir.path().join(SUCCESS_FILENAME)).unwrap();
        let dest = local_dest(dir.path());

        let v = verify_at_destination(&dest, "", ValidateDepth::Full).unwrap();
        assert!(v.manifest_found);
        assert!(
            !v.success_marker_consistent,
            "no marker => false (no signal)"
        );
        assert!(v.passed);
        assert!(v.failures.is_empty());
    }

    #[test]
    fn a_manifest_whose_last_run_failed_does_not_pass_validation() {
        let dir = tempfile::tempdir().unwrap();
        let m = build_manifest(vec![], ManifestStatus::Failed);
        write_dataset(dir.path(), &m, &[]);
        assert!(!dir.path().join(SUCCESS_FILENAME).exists());
        let dest = local_dest(dir.path());

        let v = verify_at_destination(&dest, "", ValidateDepth::Full).unwrap();
        assert!(
            !v.passed,
            "a failed run's manifest must not validate as PASSED"
        );
        assert_eq!(
            v.failures,
            vec![Failure::RunNotSuccessful {
                status: "failed".into()
            }]
        );
        assert_eq!(
            v.failures[0].to_string(),
            "the manifest records its last run as failed, not success: this prefix \
             does not hold a completed export. Re-run the export."
        );
    }

    #[test]
    fn self_inconsistent_manifest_is_flagged_but_part_check_still_runs() {
        let dir = tempfile::tempdir().unwrap();
        let mut m = build_manifest(
            vec![part(1, 10, 4, "xxh3:1111111111111111")],
            ManifestStatus::Success,
        );
        m.row_count = 9999;

        let body = serde_json::to_vec_pretty(&m).unwrap();
        std::fs::write(dir.path().join("part-000001.parquet"), b"AAAA").unwrap();
        std::fs::write(dir.path().join(MANIFEST_FILENAME), &body).unwrap();
        std::fs::write(
            dir.path().join(SUCCESS_FILENAME),
            success_marker_body(&body),
        )
        .unwrap();
        let dest = local_dest(dir.path());

        let v = verify_at_destination(&dest, "", ValidateDepth::Full).unwrap();
        assert!(v.manifest_found);
        assert!(!v.manifest_self_consistent);
        assert!(!v.passed);
        assert_eq!(v.parts_verified, 1);
        assert!(
            v.failures
                .iter()
                .any(|f| matches!(f, Failure::ManifestSelfInconsistent { .. }))
        );
    }

    /// A `manifest.json` that is not valid JSON at all.
    #[test]
    fn an_unparseable_manifest_is_found_and_carries_a_reason() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("part-000001.parquet"), b"AAAA").unwrap();
        std::fs::write(dir.path().join(MANIFEST_FILENAME), b"{ this is not json").unwrap();
        let dest = local_dest(dir.path());

        let v = verify_at_destination(&dest, "", ValidateDepth::Full).unwrap();

        assert!(
            v.manifest_found,
            "an unreadable manifest.json is still a manifest that EXISTS — \
             reporting it absent sends the operator to the legacy-run path \
             instead of at the corruption: {v:?}"
        );
        assert!(
            !v.legacy_run,
            "a corrupt manifest is not a legacy run: {v:?}"
        );
        assert!(
            v.failures
                .iter()
                .any(|f| matches!(f, Failure::ManifestSelfInconsistent { .. })),
            "a parse failure must be RECORDED as a failure — a red verdict with \
             an empty `failures` gives the operator no reason at all: {v:?}"
        );
        assert!(
            !v.passed,
            "a destination whose manifest cannot be parsed has not passed: {v:?}"
        );

        let detail = v
            .failures
            .iter()
            .find_map(|f| match f {
                Failure::ManifestSelfInconsistent { detail } => Some(detail.clone()),
                _ => None,
            })
            .expect("asserted present above");
        assert!(
            detail.contains("manifest.json"),
            "the reason must name the file it is about: {detail}"
        );
    }

    #[test]
    fn verify_counters_track_present_and_failed_parts() {
        let dir = tempfile::tempdir().unwrap();
        let m = build_manifest(
            vec![
                part(1, 10, 4, "xxh3:1111111111111111"),
                part(2, 10, 4, "xxh3:2222222222222222"),
            ],
            ManifestStatus::Success,
        );
        write_dataset(dir.path(), &m, &[("part-000001.parquet", b"AAAA")]);
        let dest = local_dest(dir.path());

        let v = verify_at_destination(&dest, "", ValidateDepth::Full).unwrap();
        assert!(v.manifest_found, "manifest is at the prefix");
        assert_eq!(v.parts_verified, 1, "part 1 is present at its size");
        assert_eq!(v.parts_failed, 1, "part 2 is missing");
        assert!(
            v.failures
                .iter()
                .any(|f| matches!(f, Failure::PartMissing { part_id: 2, .. })),
            "the missing part is named in failures: {:?}",
            v.failures
        );
        assert!(!v.passed, "a missing part must fail the verdict");
    }

    #[test]
    fn read_capped_boundary_is_exact() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("at-cap.bin"), vec![0u8; 8]).unwrap();
        std::fs::write(dir.path().join("over-cap.bin"), vec![0u8; 9]).unwrap();
        let dest = local_dest(dir.path());
        assert_eq!(
            read_capped(&dest, "at-cap.bin", 8)
                .expect("exactly at cap loads")
                .len(),
            8
        );
        let err = read_capped(&dest, "over-cap.bin", 8).expect_err("over cap refuses");
        assert!(
            err.to_string().contains("read cap"),
            "actionable message: {err:#}"
        );
        crate::error::assert_coded(&err, "RIVET_DEST_CONTROL_ARTIFACT_INVALID", 5);
        let err = read_capped(&dest, "absent.bin", 8).expect_err("an absent object errors");
        crate::error::assert_coded(&err, "RIVET_DEST_OBJECT_NOT_FOUND", 2);
    }

    #[test]
    fn preview_truncates_only_past_forty_chars() {
        let s39: String = "x".repeat(39);
        let s40: String = "x".repeat(40);
        let s41: String = "x".repeat(41);
        assert_eq!(preview(&s39), s39, "under the limit: unchanged");
        assert_eq!(
            preview(&s40),
            s40,
            "exactly at the limit: unchanged (`>` not `>=`)"
        );
        assert_eq!(
            preview(&s41),
            format!("{s40}\u{2026}"),
            "past the limit: 40 chars + ellipsis"
        );
    }

    #[test]
    fn untracked_object_under_prefix_is_flagged() {
        let dir = tempfile::tempdir().unwrap();
        let m = build_manifest(
            vec![part(1, 10, 4, "xxh3:1111111111111111")],
            ManifestStatus::Success,
        );
        write_dataset(dir.path(), &m, &[("part-000001.parquet", b"AAAA")]);
        std::fs::write(dir.path().join("rogue.parquet"), b"XX").unwrap();
        let dest = local_dest(dir.path());

        let v = verify_at_destination(&dest, "", ValidateDepth::Full).unwrap();
        assert!(
            v.failures.iter().any(
                |f| matches!(f, Failure::UntrackedObject { key, .. } if key == "rogue.parquet")
            )
        );
        assert!(v.passed);
    }

    #[test]
    fn quarantine_prefix_objects_are_silently_ignored() {
        let dir = tempfile::tempdir().unwrap();
        let m = build_manifest(
            vec![part(1, 10, 4, "xxh3:1111111111111111")],
            ManifestStatus::Success,
        );
        write_dataset(dir.path(), &m, &[("part-000001.parquet", b"AAAA")]);
        std::fs::create_dir_all(dir.path().join(crate::manifest::QUARANTINE_PREFIX)).unwrap();
        std::fs::write(
            dir.path()
                .join(crate::manifest::QUARANTINE_PREFIX)
                .join("old.parquet"),
            b"OO",
        )
        .unwrap();
        let dest = local_dest(dir.path());

        let v = verify_at_destination(&dest, "", ValidateDepth::Full).unwrap();
        assert!(v.passed);
        assert!(
            !v.failures
                .iter()
                .any(|f| matches!(f, Failure::UntrackedObject { .. })),
            "quarantine_prefix is the legitimate home for these — must not flag"
        );
    }

    #[test]
    fn doctor_probe_is_not_flagged_as_untracked() {
        let dir = tempfile::tempdir().unwrap();
        let m = build_manifest(
            vec![part(1, 10, 4, "xxh3:1111111111111111")],
            ManifestStatus::Success,
        );
        write_dataset(dir.path(), &m, &[("part-000001.parquet", b"AAAA")]);
        std::fs::write(
            dir.path().join(crate::manifest::DOCTOR_PROBE_FILENAME),
            b"ok",
        )
        .unwrap();
        let dest = local_dest(dir.path());

        let v = verify_at_destination(&dest, "", ValidateDepth::Full).unwrap();
        assert!(
            !v.has_failures(),
            "doctor probe must not surface as a failure: {:?}",
            v.failures
        );
        assert!(v.passed);
    }

    #[test]
    fn verifies_in_subdirectory_when_manifest_dir_is_non_empty() {
        let outer = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(outer.path().join("sub/run")).unwrap();
        let m = build_manifest(
            vec![part(1, 10, 4, "xxh3:1111111111111111")],
            ManifestStatus::Success,
        );
        let body = serde_json::to_vec_pretty(&m).unwrap();
        std::fs::write(outer.path().join("sub/run/part-000001.parquet"), b"AAAA").unwrap();
        std::fs::write(outer.path().join("sub/run").join(MANIFEST_FILENAME), &body).unwrap();
        std::fs::write(
            outer.path().join("sub/run").join(SUCCESS_FILENAME),
            success_marker_body(&body),
        )
        .unwrap();
        let dest = local_dest(outer.path());

        let v = verify_at_destination(&dest, "sub/run", ValidateDepth::Full).unwrap();
        assert!(v.passed);
        assert_eq!(v.parts_verified, 1);

        let v2 = verify_at_destination(&dest, "sub/run/", ValidateDepth::Full).unwrap();
        assert!(v2.passed);
    }

    /// Wraps a real `LocalDestination` but fails every `list_prefix`.
    struct ListFails(LocalDestination);
    impl crate::destination::Destination for ListFails {
        fn write(&self, p: &Path, k: &str) -> Result<crate::destination::WriteOutcome> {
            self.0.write(p, k)
        }
        fn capabilities(&self) -> crate::destination::DestinationCapabilities {
            self.0.capabilities()
        }
        fn head(&self, k: &str) -> Result<Option<crate::destination::ObjectMeta>> {
            self.0.head(k)
        }
        fn read(&self, k: &str) -> Result<Vec<u8>> {
            self.0.read(k)
        }
        fn list_prefix(&self, _: &str) -> Result<Vec<crate::destination::ObjectMeta>> {
            anyhow::bail!("listing unavailable")
        }
    }

    #[test]
    fn list_failure_cannot_certify_parts_and_fails_the_audit() {
        let dir = tempfile::tempdir().unwrap();
        let m = build_manifest(vec![part(0, 3, 3, "xxh3:0")], ManifestStatus::Success);
        write_dataset(dir.path(), &m, &[("part-000000.parquet", b"abc")]);
        let dest = ListFails(local_dest(dir.path()));

        let v = verify_at_destination(&dest, "", ValidateDepth::Full).unwrap();
        assert!(v.manifest_found);
        assert!(v.manifest_self_consistent);
        assert!(
            !v.passed,
            "an audit that cannot list the prefix must not pass"
        );
        assert_eq!(v.parts_verified, 0);
        assert!(
            v.failures
                .iter()
                .any(|f| matches!(f, Failure::ListPrefixError { .. })),
            "expected a ListPrefixError, got: {:?}",
            v.failures
        );
    }

    /// Wraps a real `LocalDestination` but fails reading `manifest.json` (head still sees it).
    struct ManifestReadFails(LocalDestination);
    impl crate::destination::Destination for ManifestReadFails {
        fn write(&self, p: &Path, k: &str) -> Result<crate::destination::WriteOutcome> {
            self.0.write(p, k)
        }
        fn capabilities(&self) -> crate::destination::DestinationCapabilities {
            self.0.capabilities()
        }
        fn head(&self, k: &str) -> Result<Option<crate::destination::ObjectMeta>> {
            self.0.head(k)
        }
        fn read(&self, k: &str) -> Result<Vec<u8>> {
            if k.ends_with(MANIFEST_FILENAME) {
                anyhow::bail!("permission denied (simulated)")
            }
            self.0.read(k)
        }
        fn list_prefix(&self, p: &str) -> Result<Vec<crate::destination::ObjectMeta>> {
            self.0.list_prefix(p)
        }
    }

    #[test]
    fn unreadable_manifest_is_explicit_failure_not_legacy() {
        let dir = tempfile::tempdir().unwrap();
        let m = build_manifest(
            vec![part(1, 10, 4, "xxh3:1111111111111111")],
            ManifestStatus::Success,
        );
        write_dataset(dir.path(), &m, &[("part-000001.parquet", b"AAAA")]);
        let dest = ManifestReadFails(local_dest(dir.path()));

        let v = verify_at_destination(&dest, "", ValidateDepth::Full).unwrap();
        assert!(!v.manifest_found);
        assert!(!v.legacy_run, "a read error is not the M6 legacy label");
        assert!(!v.passed);
        assert!(v.has_failures(), "orchestrators need a reason to refuse");
        assert!(
            matches!(v.failures.as_slice(), [Failure::ManifestReadError { .. }]),
            "expected exactly one ManifestReadError, got: {:?}",
            v.failures
        );
    }

    /// Same contract when `head` itself errors (cannot even stat the manifest).
    struct ManifestHeadFails(LocalDestination);
    impl crate::destination::Destination for ManifestHeadFails {
        fn write(&self, p: &Path, k: &str) -> Result<crate::destination::WriteOutcome> {
            self.0.write(p, k)
        }
        fn capabilities(&self) -> crate::destination::DestinationCapabilities {
            self.0.capabilities()
        }
        fn head(&self, k: &str) -> Result<Option<crate::destination::ObjectMeta>> {
            if k.ends_with(MANIFEST_FILENAME) {
                anyhow::bail!("io timeout (simulated)")
            }
            self.0.head(k)
        }
        fn read(&self, k: &str) -> Result<Vec<u8>> {
            self.0.read(k)
        }
        fn list_prefix(&self, p: &str) -> Result<Vec<crate::destination::ObjectMeta>> {
            self.0.list_prefix(p)
        }
    }

    #[test]
    fn manifest_head_error_is_explicit_failure_not_legacy() {
        let dir = tempfile::tempdir().unwrap();
        let m = build_manifest(
            vec![part(1, 10, 4, "xxh3:1111111111111111")],
            ManifestStatus::Success,
        );
        write_dataset(dir.path(), &m, &[("part-000001.parquet", b"AAAA")]);
        let dest = ManifestHeadFails(local_dest(dir.path()));

        let v = verify_at_destination(&dest, "", ValidateDepth::Full).unwrap();
        assert!(!v.manifest_found);
        assert!(!v.legacy_run);
        assert!(!v.passed);
        assert!(
            matches!(
                v.failures.as_slice(),
                [Failure::ManifestReadError { detail }] if detail.contains("manifest head failed")
            ),
            "expected one ManifestReadError naming the head step, got: {:?}",
            v.failures
        );
    }

    struct SuccessMarkerOversized(LocalDestination);
    impl crate::destination::Destination for SuccessMarkerOversized {
        fn write(&self, p: &Path, k: &str) -> Result<crate::destination::WriteOutcome> {
            self.0.write(p, k)
        }
        fn capabilities(&self) -> crate::destination::DestinationCapabilities {
            self.0.capabilities()
        }
        fn head(&self, k: &str) -> Result<Option<crate::destination::ObjectMeta>> {
            if k.ends_with(crate::manifest::SUCCESS_FILENAME) {
                return Ok(Some(crate::destination::ObjectMeta {
                    key: k.to_string(),
                    size_bytes: SUCCESS_MARKER_MAX_BYTES * 4,
                    content_md5: None,
                }));
            }
            self.0.head(k)
        }
        fn read(&self, k: &str) -> Result<Vec<u8>> {
            assert!(
                !k.ends_with(crate::manifest::SUCCESS_FILENAME),
                "verify must NOT read an oversized _SUCCESS — the size cap must short-circuit \
                 before materialising it into memory (the OOM this guards)"
            );
            self.0.read(k)
        }
        fn list_prefix(&self, p: &str) -> Result<Vec<crate::destination::ObjectMeta>> {
            self.0.list_prefix(p)
        }
    }

    #[test]
    fn oversized_success_marker_is_malformed_and_never_read() {
        let dir = tempfile::tempdir().unwrap();
        let m = build_manifest(
            vec![part(1, 10, 4, "xxh3:1111111111111111")],
            ManifestStatus::Success,
        );
        write_dataset(dir.path(), &m, &[("part-000001.parquet", b"AAAA")]);
        let dest = SuccessMarkerOversized(local_dest(dir.path()));

        let v = verify_at_destination(&dest, "", ValidateDepth::Full).unwrap();
        assert!(
            v.failures.iter().any(|f| matches!(
                f,
                Failure::SuccessMarkerMalformed { body_preview } if body_preview.contains("oversized")
            )),
            "an oversized _SUCCESS must yield SuccessMarkerMalformed(oversized) without reading it, got: {:?}",
            v.failures
        );
    }

    #[test]
    fn passed_is_derived_advisory_failures_do_not_fail() {
        let mut v = ManifestVerification {
            manifest_found: true,
            ..ManifestVerification::empty()
        };
        v.failures.push(Failure::UntrackedObject {
            key: "stray.parquet".into(),
            size_bytes: 9,
        });
        v.recompute_passed();
        assert!(v.passed, "untracked surplus is advisory, not fatal");

        v.failures.push(Failure::PartMissing {
            part_id: 1,
            path: "part-000001.parquet".into(),
        });
        v.recompute_passed();
        assert!(!v.passed, "a missing part is fatal");

        let mut legacy = ManifestVerification::empty();
        legacy.recompute_passed();
        assert!(!legacy.passed, "no manifest found → cannot certify");
    }

    #[test]
    fn verify_content_policy_fails_only_size_only_parts() {
        let base = ManifestVerification {
            manifest_found: true,
            parts_verified: 3,
            parts_md5_verified: 2,
            ..ManifestVerification::empty()
        };
        let mut sz = base.clone();
        sz.recompute_passed();
        sz.enforce_content_policy(false);
        assert!(sz.passed, "size-only OK under verify: size");

        let mut ct = base.clone();
        ct.recompute_passed();
        ct.enforce_content_policy(true);
        assert!(!ct.passed, "a size-only part fails verify: content");
        assert!(
            ct.failures.iter().any(|f| matches!(
                f,
                Failure::ContentVerificationUnmet {
                    size_only: 1,
                    total: 3
                }
            )),
            "expected ContentVerificationUnmet, got: {:?}",
            ct.failures
        );

        let mut all = ManifestVerification {
            parts_md5_verified: 3,
            ..base
        };
        all.recompute_passed();
        all.enforce_content_policy(true);
        assert!(
            all.passed && all.failures.is_empty(),
            "all md5 meets verify: content"
        );
    }

    #[test]
    fn require_manifest_escalates_legacy_to_fatal_absent() {
        let mut v = ManifestVerification::legacy();
        assert!(v.legacy_run && !v.has_failures());

        v.require_manifest_present("exports/2026-06-09/orders/");

        assert!(!v.legacy_run, "no longer the benign legacy-run label");
        assert!(!v.passed, "an absent-but-required manifest cannot pass");
        assert!(
            matches!(
                v.failures.as_slice(),
                [Failure::ManifestRequiredButAbsent { prefix }]
                    if prefix == "exports/2026-06-09/orders/"
            ),
            "expected one ManifestRequiredButAbsent naming the prefix, got: {:?}",
            v.failures
        );
    }

    #[test]
    fn a_partition_without_a_manifest_is_a_fatal_failure_naming_the_export() {
        let mut v = ManifestVerification::legacy();
        v.require_partition_delivered("out/created_at=2026-03-02", "events");
        assert!(!v.legacy_run && !v.passed);
        assert_eq!(
            v.failures.as_slice(),
            [Failure::PartitionNotDelivered {
                prefix: "out/created_at=2026-03-02".into(),
                export: "events".into(),
            }]
        );
        assert_eq!(
            v.failures[0].to_string(),
            "no manifest at out/created_at=2026-03-02: the source holds this partition of \
             `events` but the destination holds none of it — it was never delivered or its \
             directory was removed. Re-deliver it with `rivet run --export events`"
        );
        assert!(v.has_verified_wrong_failure());
    }

    #[test]
    fn a_recorded_delivery_with_no_manifest_is_a_fatal_failure_naming_export_and_remedy() {
        let mut v = ManifestVerification::legacy();
        assert!(v.is_bare_legacy());
        v.require_recorded_delivery("/data/out", "orders");
        assert!(!v.legacy_run && !v.passed && !v.is_bare_legacy());
        assert_eq!(
            v.failures.as_slice(),
            [Failure::RecordedDeliveryAbsent {
                prefix: "/data/out".into(),
                export: "orders".into(),
            }]
        );
        assert_eq!(
            v.failures[0].to_string(),
            "no manifest at /data/out: the state records a successful run of `orders` here and \
             the destination holds none of it — the delivery was removed or the destination \
             moved. Restore the prefix from a backup, or deliver it again with `rivet run \
             --export orders` (for an incremental or chunked export run `rivet state reset \
             --export orders` first, so the run starts over instead of continuing from its \
             stored progress)"
        );
        assert!(v.has_verified_wrong_failure());
    }

    #[test]
    fn a_delivery_the_state_records_is_not_asserted_over_a_found_manifest_or_a_read_error() {
        let mut found = ManifestVerification {
            manifest_found: true,
            legacy_run: false,
            passed: true,
            ..ManifestVerification::legacy()
        };
        found.require_recorded_delivery("p", "e");
        assert!(found.passed && found.failures.is_empty());
        let mut unread = ManifestVerification {
            legacy_run: false,
            failures: vec![Failure::ManifestReadError { detail: "x".into() }],
            ..ManifestVerification::legacy()
        };
        unread.require_recorded_delivery("p", "e");
        assert_eq!(unread.failures.len(), 1);
    }

    #[test]
    fn a_delivered_partition_and_a_read_error_are_left_alone() {
        let mut found = ManifestVerification {
            manifest_found: true,
            manifest_self_consistent: true,
            parts_verified: 1,
            passed: true,
            ..ManifestVerification::empty()
        };
        found.require_partition_delivered("p", "events");
        assert!(found.passed && found.failures.is_empty());

        let mut unreadable = ManifestVerification::legacy();
        unreadable.legacy_run = false;
        unreadable.failures.push(Failure::ManifestReadError {
            detail: "permission denied".into(),
        });
        unreadable.recompute_passed();
        unreadable.require_partition_delivered("p", "events");
        assert!(matches!(
            unreadable.failures.as_slice(),
            [Failure::ManifestReadError { .. }]
        ));
    }

    #[test]
    fn require_manifest_is_noop_on_a_real_passing_manifest() {
        let mut v = ManifestVerification {
            manifest_found: true,
            manifest_self_consistent: true,
            parts_verified: 1,
            passed: true,
            ..ManifestVerification::empty()
        };
        v.require_manifest_present("exports/orders/");
        assert!(
            v.passed && v.failures.is_empty(),
            "real dataset still passes"
        );
    }

    #[test]
    fn require_manifest_does_not_double_flag_a_read_error() {
        let mut v = ManifestVerification::legacy();
        v.legacy_run = false;
        v.failures.push(Failure::ManifestReadError {
            detail: "permission denied".into(),
        });
        v.recompute_passed();

        v.require_manifest_present("exports/orders/");

        assert!(
            matches!(v.failures.as_slice(), [Failure::ManifestReadError { .. }]),
            "must leave the existing read-error verdict alone, got: {:?}",
            v.failures
        );
    }

    #[test]
    fn light_depth_skips_part_reconcile_even_when_a_part_is_missing() {
        let dir = tempfile::tempdir().unwrap();
        let m = build_manifest(
            vec![
                part(1, 10, 4, "xxh3:1111111111111111"),
                part(2, 20, 5, "xxh3:2222222222222222"),
            ],
            ManifestStatus::Success,
        );
        write_dataset(dir.path(), &m, &[]);
        let dest = local_dest(dir.path());

        let v = verify_at_destination(&dest, "", ValidateDepth::Light).unwrap();
        assert_eq!(v.depth_level, "light");
        assert_eq!(
            v.parts_verified, 0,
            "light skips the listing — no part is ever verified"
        );
        assert_eq!(
            v.parts_failed, 0,
            "no part reconcile means no part failures"
        );
        assert!(
            !v.failures.iter().any(|f| matches!(
                f,
                Failure::PartMissing { .. } | Failure::ListPrefixError { .. }
            )),
            "light must not surface part or list failures, got: {:?}",
            v.failures
        );
        assert!(
            v.success_marker_consistent,
            "_SUCCESS is still checked at light depth"
        );
        assert!(v.manifest_self_consistent);
        assert!(
            v.passed,
            "manifest + _SUCCESS are consistent, so a light pass certifies it"
        );
    }

    #[test]
    fn light_depth_never_lists_so_a_list_failure_cannot_trip() {
        let dir = tempfile::tempdir().unwrap();
        let m = build_manifest(vec![part(0, 3, 3, "xxh3:0")], ManifestStatus::Success);
        write_dataset(dir.path(), &m, &[("part-000000.parquet", b"abc")]);
        let dest = ListFails(local_dest(dir.path()));

        let v = verify_at_destination(&dest, "", ValidateDepth::Light).unwrap();
        assert_eq!(v.depth_level, "light");
        assert!(
            !v.failures
                .iter()
                .any(|f| matches!(f, Failure::ListPrefixError { .. })),
            "light never lists, so a failing list_prefix cannot surface, got: {:?}",
            v.failures
        );
        assert_eq!(v.parts_verified, 0);
        assert!(v.passed, "manifest + _SUCCESS consistent → light passes");
    }

    #[test]
    fn sample_depth_runs_part_reconcile_like_full() {
        let dir = tempfile::tempdir().unwrap();
        let m = build_manifest(
            vec![
                part(1, 10, 4, "xxh3:1111111111111111"),
                part(2, 20, 5, "xxh3:2222222222222222"),
            ],
            ManifestStatus::Success,
        );
        write_dataset(dir.path(), &m, &[("part-000001.parquet", b"AAAA")]);
        let dest = local_dest(dir.path());

        let v = verify_at_destination(&dest, "", ValidateDepth::Sample).unwrap();
        assert_eq!(v.depth_level, "sample");
        assert_eq!(v.parts_verified, 1);
        assert_eq!(v.parts_failed, 1);
        assert!(!v.passed);
        assert!(
            v.failures
                .iter()
                .any(|f| matches!(f, Failure::PartMissing { part_id: 2, .. })),
            "sample reconciles parts just like full, got: {:?}",
            v.failures
        );
    }

    #[test]
    fn depth_level_is_stamped_on_every_verdict_shape() {
        let dir = tempfile::tempdir().unwrap();
        let dest = local_dest(dir.path());
        for depth in [
            ValidateDepth::Light,
            ValidateDepth::Sample,
            ValidateDepth::Full,
        ] {
            let v = verify_at_destination(&dest, "", depth).unwrap();
            assert!(v.legacy_run, "empty prefix is the legacy shape");
            assert_eq!(
                v.depth_level,
                depth.label(),
                "legacy verdict must still carry its depth label"
            );
        }
    }

    #[test]
    fn error_code_is_stable_and_distinct_per_variant() {
        let cases: &[(Failure, &str)] = &[
            (
                Failure::PartMissing {
                    part_id: 1,
                    path: "p".into(),
                },
                "RIVET_VERIFY_PART_MISSING",
            ),
            (
                Failure::PartSizeMismatch {
                    part_id: 1,
                    path: "p".into(),
                    expected: 1,
                    actual: 2,
                },
                "RIVET_VERIFY_PART_SIZE_MISMATCH",
            ),
            (
                Failure::PartChecksumMismatch {
                    part_id: 1,
                    path: "p".into(),
                    expected: "a".into(),
                    actual: "b".into(),
                },
                "RIVET_VERIFY_PART_CHECKSUM_MISMATCH",
            ),
            (
                Failure::SuccessMarkerMalformed {
                    body_preview: "x".into(),
                },
                "RIVET_VERIFY_SUCCESS_MALFORMED",
            ),
            (
                Failure::SuccessMarkerStale {
                    marker_fingerprint: "a".into(),
                    manifest_fingerprint: "b".into(),
                },
                "RIVET_VERIFY_SUCCESS_STALE",
            ),
            (
                Failure::ManifestSelfInconsistent { detail: "d".into() },
                "RIVET_VERIFY_MANIFEST_INCONSISTENT",
            ),
            (
                Failure::ManifestReadError { detail: "d".into() },
                "RIVET_VERIFY_MANIFEST_READ_ERROR",
            ),
            (
                Failure::SuccessMarkerReadError { detail: "d".into() },
                "RIVET_VERIFY_SUCCESS_READ_ERROR",
            ),
            (
                Failure::ListPrefixError { detail: "d".into() },
                "RIVET_VERIFY_LIST_ERROR",
            ),
            (
                Failure::UntrackedObject {
                    key: "k".into(),
                    size_bytes: 1,
                },
                "RIVET_VERIFY_UNTRACKED_OBJECT",
            ),
            (
                Failure::ContentVerificationUnmet {
                    size_only: 1,
                    total: 2,
                },
                "RIVET_VERIFY_CONTENT_UNMET",
            ),
            (
                Failure::ManifestRequiredButAbsent { prefix: "p".into() },
                "RIVET_VERIFY_MANIFEST_REQUIRED",
            ),
            (
                Failure::ManifestMissingUnderMarker { prefix: "p".into() },
                "RIVET_VERIFY_MANIFEST_MISSING",
            ),
            (
                Failure::PartitionNotDelivered {
                    prefix: "p".into(),
                    export: "e".into(),
                },
                "RIVET_VERIFY_PARTITION_NOT_DELIVERED",
            ),
            (
                Failure::RecordedDeliveryAbsent {
                    prefix: "p".into(),
                    export: "e".into(),
                },
                "RIVET_VERIFY_DELIVERY_ABSENT",
            ),
            (
                Failure::ValueChecksumMismatch {
                    detail: "flip".into(),
                },
                "RIVET_VERIFY_VALUE_CHECKSUM",
            ),
            (
                Failure::CdcPositionViolation {
                    detail: "gap".into(),
                },
                "RIVET_VERIFY_CDC_POSITION",
            ),
            (
                Failure::PartRowCountMismatch {
                    detail: "declared 500, holds 400".into(),
                },
                "RIVET_VERIFY_PART_ROW_COUNT",
            ),
            (
                Failure::RunNotSuccessful {
                    status: "failed".into(),
                },
                "RIVET_VERIFY_RUN_NOT_SUCCESSFUL",
            ),
            (
                Failure::ValueCheckNotAvailable {
                    format: "csv".into(),
                },
                "RIVET_VERIFY_VALUE_CHECK_NOT_AVAILABLE",
            ),
        ];
        for (failure, code) in cases {
            assert_eq!(&failure.error_code(), code, "code for {failure:?}");
            assert!(
                failure.error_code().starts_with("RIVET_VERIFY_"),
                "every code shares the RIVET_VERIFY_ prefix"
            );
        }

        let src = include_str!("validate_manifest.rs");
        let enum_body = {
            let at = src
                .find("pub enum Failure {")
                .expect("the Failure enum must be declared here");
            let rest = &src[at..];
            &rest[..rest.find("\n}").expect("the enum must close")]
        };
        let declared: Vec<&str> = enum_body
            .lines()
            .skip(1)
            .filter(|l| l.starts_with("    ") && !l.starts_with("     "))
            .filter_map(|l| {
                let t = l.trim();
                t.chars().next().filter(|c| c.is_ascii_uppercase())?;
                Some(t.split(|c: char| !c.is_alphanumeric()).next().unwrap_or(""))
            })
            .filter(|v| !v.is_empty())
            .collect();
        assert!(
            declared.len() >= 12,
            "parsed only {} variant(s) from `Failure` — an empty dimension would \
             make this assertion vacuous, which is the defect it replaces: {declared:?}",
            declared.len()
        );
        let covered: Vec<&str> = cases
            .iter()
            .map(|(f, _)| match f {
                Failure::PartMissing { .. } => "PartMissing",
                Failure::PartSizeMismatch { .. } => "PartSizeMismatch",
                Failure::PartChecksumMismatch { .. } => "PartChecksumMismatch",
                Failure::ValueChecksumMismatch { .. } => "ValueChecksumMismatch",
                Failure::PartRowCountMismatch { .. } => "PartRowCountMismatch",
                Failure::CdcPositionViolation { .. } => "CdcPositionViolation",
                Failure::SuccessMarkerMalformed { .. } => "SuccessMarkerMalformed",
                Failure::SuccessMarkerStale { .. } => "SuccessMarkerStale",
                Failure::ManifestSelfInconsistent { .. } => "ManifestSelfInconsistent",
                Failure::ManifestReadError { .. } => "ManifestReadError",
                Failure::SuccessMarkerReadError { .. } => "SuccessMarkerReadError",
                Failure::ListPrefixError { .. } => "ListPrefixError",
                Failure::UntrackedObject { .. } => "UntrackedObject",
                Failure::ContentVerificationUnmet { .. } => "ContentVerificationUnmet",
                Failure::ManifestRequiredButAbsent { .. } => "ManifestRequiredButAbsent",
                Failure::PartitionNotDelivered { .. } => "PartitionNotDelivered",
                Failure::RecordedDeliveryAbsent { .. } => "RecordedDeliveryAbsent",
                Failure::ManifestMissingUnderMarker { .. } => "ManifestMissingUnderMarker",
                Failure::RunNotSuccessful { .. } => "RunNotSuccessful",
                Failure::ValueCheckNotAvailable { .. } => "ValueCheckNotAvailable",
            })
            .collect();
        let missing: Vec<&&str> = declared.iter().filter(|v| !covered.contains(v)).collect();
        assert!(
            missing.is_empty(),
            "`Failure` declares {} variant(s); these have no error-code case, so \
             renaming their code would break a CI gate silently: {missing:?}",
            declared.len()
        );
    }

    #[test]
    fn cdc_position_violation_is_verified_wrong_not_could_not_verify() {
        let viol = Failure::CdcPositionViolation {
            detail: "export 'x': pos gap 5 -> 8".into(),
        };
        assert!(
            !viol.is_could_not_verify(),
            "a __pos violation is verified-wrong, not could-not-verify"
        );
        assert!(viol.is_fatal(), "a __pos violation fails the verdict");

        let mut v = ManifestVerification::empty();
        v.passed = false;
        v.failures.push(viol);
        assert!(
            v.has_verified_wrong_failure(),
            "a verdict carrying a __pos violation must classify as verified-wrong (exit 3)"
        );

        let mut op = ManifestVerification::empty();
        op.passed = false;
        op.failures.push(Failure::ManifestReadError {
            detail: "permission denied".into(),
        });
        assert!(
            !op.has_verified_wrong_failure(),
            "a read error alone is could-not-verify (exit 1), not verified-wrong"
        );
    }
}
