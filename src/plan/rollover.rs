//! Keeping one output file inside a load job's partition budget.

use std::collections::HashSet;

use chrono::{DateTime, Datelike};

use crate::config::load::Granularity;

/// The partition bucket rows with a NULL key occupy.
pub const NULL_BUCKET: i64 = i64::MIN;

/// BigQuery's cap on partitions one load job may write.
pub(crate) const MAX_PARTITIONS_PER_JOB: i64 = 4000;

/// What the extract must know to keep a part loadable.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PartitionRollover {
    pub column: String,
    pub granularity: Granularity,
    pub cap: usize,
}

/// Parquet footer key under which the writer records the distinct partitions a part holds.
pub const PARTITION_BUCKETS_KEY: &str = "rivet.partition_buckets";

/// The footer note: `<column>|<granularity>|<distinct buckets>`.
pub fn partition_buckets_note(column: &str, granularity: Granularity, buckets: usize) -> String {
    format!("{column}|{}|{buckets}", granularity.as_str())
}

/// The note read back as `(column, granularity, buckets)`; `None` for anything else.
pub fn parse_partition_buckets_note(note: &str) -> Option<(&str, &str, i64)> {
    let mut parts = note.rsplitn(3, '|');
    let (buckets, granularity, column) = (parts.next()?, parts.next()?, parts.next()?);
    Some((column, granularity, buckets.parse().ok()?))
}

/// How a partition column's values are stored, for the conversion to epoch seconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionUnit {
    Days,
    Seconds,
    Millis,
    Micros,
    Nanos,
}

/// A stored value as epoch seconds.
pub fn to_epoch_seconds(value: i64, unit: PartitionUnit) -> i64 {
    match unit {
        PartitionUnit::Days => value.saturating_mul(86_400),
        PartitionUnit::Seconds => value,
        PartitionUnit::Millis => value.div_euclid(1_000),
        PartitionUnit::Micros => value.div_euclid(1_000_000),
        PartitionUnit::Nanos => value.div_euclid(1_000_000_000),
    }
}

/// The partition a value falls in, as a bucket id comparable within one granularity.
pub fn bucket_of(secs: i64, granularity: Granularity) -> i64 {
    match granularity {
        Granularity::Hour => secs.div_euclid(3_600),
        Granularity::Day => secs.div_euclid(86_400),
        Granularity::Month => {
            civil(secs).map_or(0, |t| i64::from(t.year()) * 12 + i64::from(t.month0()))
        }
        Granularity::Year => civil(secs).map_or(0, |t| i64::from(t.year())),
    }
}

fn civil(secs: i64) -> Option<DateTime<chrono::Utc>> {
    DateTime::from_timestamp(secs, 0)
}

/// How many leading rows of `buckets` the current part can still take before it would hold more than `cap` distinct partitions.
pub fn rows_that_fit(held: &HashSet<i64>, buckets: &[i64], cap: usize) -> usize {
    if cap == 0 {
        return buckets.len();
    }
    let mut added: HashSet<i64> = HashSet::new();
    for (row, bucket) in buckets.iter().enumerate() {
        if held.contains(bucket) || added.contains(bucket) {
            continue;
        }
        if held.len() + added.len() >= cap {
            return row;
        }
        added.insert(*bucket);
    }
    buckets.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_partition_column_containing_a_pipe_round_trips_through_the_footer_note() {
        let note = partition_buckets_note("event|date", Granularity::Day, 7);
        assert_eq!(
            parse_partition_buckets_note(&note),
            Some(("event|date", "day", 7))
        );
        assert_eq!(parse_partition_buckets_note("d|day|x"), None);
        assert_eq!(parse_partition_buckets_note("day|7"), None);
    }

    fn at(y: i32, m: u32, d: u32, h: u32) -> i64 {
        chrono::NaiveDate::from_ymd_opt(y, m, d)
            .unwrap()
            .and_hms_opt(h, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp()
    }

    /// Two values in the same calendar bucket are ONE partition, and the next one over is a different partition.
    #[test]
    fn bucket_identity_follows_the_calendar_per_granularity() {
        let noon = at(2024, 3, 15, 12);
        let later = at(2024, 3, 15, 13);
        assert_eq!(
            bucket_of(noon, Granularity::Day),
            bucket_of(later, Granularity::Day)
        );
        assert_ne!(
            bucket_of(noon, Granularity::Hour),
            bucket_of(later, Granularity::Hour)
        );

        let leap = at(2024, 2, 29, 0);
        let march = at(2024, 3, 1, 0);
        assert_eq!(
            bucket_of(march, Granularity::Day) - bucket_of(leap, Granularity::Day),
            1
        );
        assert_eq!(
            bucket_of(march, Granularity::Month) - bucket_of(leap, Granularity::Month),
            1
        );
        assert_eq!(bucket_of(leap, Granularity::Year), 2024);

        let dec = at(2023, 12, 31, 23);
        let jan = at(2024, 1, 1, 0);
        assert_eq!(
            bucket_of(jan, Granularity::Month) - bucket_of(dec, Granularity::Month),
            1
        );
        assert_eq!(
            bucket_of(jan, Granularity::Year) - bucket_of(dec, Granularity::Year),
            1
        );
    }

    /// Values before the epoch keep going down rather than folding onto zero.
    #[test]
    fn buckets_stay_ordered_before_the_epoch() {
        let before = at(1969, 12, 31, 12);
        let after = at(1970, 1, 1, 12);
        assert_eq!(
            bucket_of(after, Granularity::Day) - bucket_of(before, Granularity::Day),
            1
        );
        assert!(bucket_of(before, Granularity::Day) < 0);
    }

    /// A zero budget must not rotate.
    #[test]
    fn a_zero_budget_never_rotates() {
        assert_eq!(rows_that_fit(&HashSet::new(), &[1, 2, 3], 0), 3);
    }

    /// The whole batch fits when it stays inside the budget, and nothing is rotated.
    #[test]
    fn a_batch_within_the_budget_fits_whole() {
        let held = HashSet::from([1, 2]);
        assert_eq!(rows_that_fit(&held, &[1, 2, 3, 3, 2], 10), 5);
    }

    /// The split lands on the row that would exceed the cap, not after it.
    #[test]
    fn the_split_row_is_the_one_that_would_exceed_the_cap() {
        let held = HashSet::from([10, 11]);
        assert_eq!(rows_that_fit(&held, &[12, 13, 14, 15], 4), 2);
    }

    /// Repeats of a bucket the part already holds cost nothing, so a wide batch over a narrow range never rotates.
    #[test]
    fn rows_repeating_a_held_bucket_consume_no_budget() {
        let held = HashSet::from([7]);
        let buckets = vec![7; 10_000];
        assert_eq!(rows_that_fit(&held, &buckets, 1), buckets.len());
    }

    /// A part already at the cap takes nothing further.
    #[test]
    fn a_full_part_accepts_no_new_bucket() {
        let held = HashSet::from([1, 2, 3]);
        assert_eq!(rows_that_fit(&held, &[4, 5], 3), 0);
        assert_eq!(rows_that_fit(&held, &[2, 2], 3), 2);
    }

    /// NULL keys share one partition between them, so they cost one bucket however many rows carry them.
    #[test]
    fn null_keys_share_a_single_bucket() {
        let held = HashSet::new();
        let buckets = vec![NULL_BUCKET, NULL_BUCKET, NULL_BUCKET];
        assert_eq!(rows_that_fit(&held, &buckets, 1), 3);
        assert_eq!(rows_that_fit(&held, &[NULL_BUCKET, 5], 1), 1);
    }
}
