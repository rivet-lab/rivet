//! First-run density probe (#148): sample the chunk key instead of believing the catalog's row estimate.

/// How a snapshot's row figure was obtained.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EstimateMethod {
    /// Below the triage line — the catalog figure is accepted as-is (no strategy boundary is near.
    CatalogTriaged,
    /// The engine's catalog figure is near-exact by construction (MSSQL `sys.partitions`).
    CatalogExact,
    /// The stratified index probe ran; the snapshot's rows are sampled.
    Probed,
    /// Small table with no usable integer key — an honest `COUNT(*)`.
    Counted,
    /// Large table with no usable key/index: the catalog figure is kept but the snapshot says so.
    Unverified,
}

impl EstimateMethod {
    pub(crate) fn label(self) -> &'static str {
        match self {
            EstimateMethod::CatalogTriaged => "catalog-triaged",
            EstimateMethod::CatalogExact => "catalog-exact",
            EstimateMethod::Probed => "probed",
            EstimateMethod::Counted => "counted",
            EstimateMethod::Unverified => "unverified",
        }
    }
}

/// One probe's outcome, carried on `TableInfo` and recorded in the snapshot.
#[derive(Debug, Clone)]
pub(crate) struct DensityProbe {
    /// The estimate init's decisions now stand on.
    pub rows: i64,
    /// Rows per key-unit across the sampled windows (1.0 = perfectly dense autoincrement.
    pub density: f64,
    pub method: EstimateMethod,
    /// The catalog's original figure, kept for the audit trail.
    pub catalog_rows: i64,
    pub k: usize,
    pub w: i64,
}

/// Tables claimed below this are candidates for tier-0 trust.
pub(crate) const TRIAGE_ROWS: i64 = 50_000;

/// Tier-0's honesty gate, pure for the mutation gate.
pub(crate) fn span_agrees_with_claim(catalog_rows: i64, span: i64) -> bool {
    span <= catalog_rows.saturating_mul(4).max(1_000)
}
/// Tier 0: is the catalog's small row claim trusted without probing?
pub(crate) fn catalog_claim_trusted(catalog_rows: i64, span: i64) -> bool {
    catalog_rows < TRIAGE_ROWS && span_agrees_with_claim(catalog_rows, span)
}

/// Default probe shape: ~1–2 s per table on a real index at any size.
pub(crate) const PROBE_K: usize = 50;
pub(crate) const PROBE_W: i64 = 10_000;

/// The K stratified window starts over `[min, max]`, each window `w` wide.
pub(crate) fn stratified_offsets(min: i64, max: i64, k: usize, w: i64) -> Vec<i64> {
    let span = max.saturating_sub(min).saturating_add(1);
    if span <= 0 || k == 0 || w <= 0 {
        return Vec::new();
    }
    if span <= w.saturating_mul(k as i64) {
        let n = ((span + w - 1) / w).max(1);
        return (0..n).map(|i| min + i * w).collect();
    }
    let k_i = k as i64;
    (0..k_i)
        .map(|i| {
            if k_i == 1 {
                min
            } else {
                min + ((span - w) as f64 * i as f64 / (k_i - 1) as f64).round() as i64
            }
        })
        .collect()
}

/// #148 sparse-key guard (roast 2026-08-09): the stratified grid can only estimate a key it actually SAMPLES.
pub(crate) fn probe_trustworthy(sampled_rows: i64, k: usize) -> bool {
    k > 0 && sampled_rows >= k as i64
}

/// `rows ≈ span × Σcount/(sampled key-units)`; density = mean rows per key-unit.
pub(crate) fn estimate_from_windows(min: i64, max: i64, counts: &[i64], w: i64) -> (i64, f64) {
    let span = max.saturating_sub(min).saturating_add(1);
    if counts.is_empty() || w <= 0 || span <= 0 {
        return (0, 0.0);
    }
    let sampled_units = (counts.len() as i64).saturating_mul(w).min(span);
    let total: i64 = counts.iter().sum();
    let density = total as f64 / sampled_units as f64;
    ((span as f64 * density).round() as i64, density)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_a_small_claim_with_an_agreeing_span_is_trusted() {
        assert!(catalog_claim_trusted(1_000, 1_000));
        assert!(
            !catalog_claim_trusted(TRIAGE_ROWS, TRIAGE_ROWS),
            "not small"
        );
        assert!(!catalog_claim_trusted(1_000, 51_000), "frozen statistics");
    }

    /// The grid covers the WHOLE span: first window at min, last window's end reaches max.
    #[test]
    fn stratified_offsets_cover_the_whole_span() {
        let (min, max, k, w) = (1_000i64, 1_000_000i64, 50usize, 10_000i64);
        let offs = stratified_offsets(min, max, k, w);
        assert_eq!(offs.len(), k);
        assert_eq!(offs[0], min, "first window starts at MIN");
        assert_eq!(
            offs[k - 1] + w - 1,
            max,
            "last window must END at MAX (fresh-id end sampled)"
        );
        for pair in offs.windows(2) {
            assert!(pair[0] < pair[1], "offsets strictly increase");
        }
    }

    /// A span smaller than K×W degrades to contiguous FULL coverage, never a widened W (the COUNT cost bound stays k×w rows).
    #[test]
    fn stratified_offsets_clamp_on_a_tiny_span() {
        let offs = stratified_offsets(10, 109, 50, 25);
        assert_eq!(offs, vec![10, 35, 60, 85], "contiguous cover, no widening");
        assert!(stratified_offsets(5, 5, 3, 10).len() == 1);
        assert!(stratified_offsets(9, 5, 3, 10).is_empty(), "inverted span");
    }

    /// The estimator: dense key → density ~1.0, rows ≈ span.
    #[test]
    fn estimate_from_windows_dense_versioned_and_gappy() {
        let w = 1_000i64;
        let (rows, d) = estimate_from_windows(1, 1_000_000, &[w; 50], w);
        assert!((d - 1.0).abs() < 1e-9);
        assert_eq!(rows, 1_000_000);
        let (rows, d) = estimate_from_windows(1, 1_000_000, &[3 * w; 50], w);
        assert!((d - 3.0).abs() < 1e-9);
        assert_eq!(rows, 3_000_000);
        let mut counts = vec![0i64; 45];
        counts.extend(vec![w; 5]);
        let (rows, d) = estimate_from_windows(1, 1_000_000, &counts, w);
        assert!((d - 0.1).abs() < 1e-9, "empty windows are data: {d}");
        assert_eq!(rows, 100_000);
    }

    /// The span gate (RED against rows-only triage.
    #[test]
    fn span_gate_catches_a_small_claim_over_a_wide_span() {
        assert!(span_agrees_with_claim(1_000, 1_000), "dense small table");
        assert!(
            span_agrees_with_claim(1_000, 3_999),
            "modest gaps tolerated"
        );
        assert!(
            !span_agrees_with_claim(1_000, 51_000),
            "the frozen-stats fixture shape MUST probe"
        );
        assert!(
            span_agrees_with_claim(100, 900),
            "the 1 000 floor for tiny tables"
        );
        assert!(!span_agrees_with_claim(100, 1_500));
    }

    /// #148: a sparse key whose windows mostly miss must NOT replace the catalog.
    #[test]
    fn probe_trustworthy_rejects_a_sparse_sample() {
        assert!(probe_trustworthy(500, 50), "dense: ~10 rows/window");
        assert!(probe_trustworthy(50, 50), "exactly the floor (1/window)");
        assert!(
            !probe_trustworthy(3, 50),
            "sparse: 3 rows across 50 windows"
        );
        assert!(!probe_trustworthy(0, 50), "all windows missed");
        assert!(!probe_trustworthy(100, 0), "no windows");
    }

    #[test]
    fn estimate_degenerate_inputs_are_zero_not_panic() {
        assert_eq!(estimate_from_windows(1, 100, &[], 10), (0, 0.0));
        assert_eq!(estimate_from_windows(1, 100, &[5], 0), (0, 0.0));
        assert_eq!(estimate_from_windows(100, 1, &[5], 10), (0, 0.0));
    }
}
