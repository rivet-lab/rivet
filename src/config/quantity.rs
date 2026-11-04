//! Numeric config knobs that count things: their range, what zero means, and the conversions the planner and the runtime read them through.

use crate::error::{Code, Result, coded, codes};

/// The default of every row-count knob: `chunk_size`, `cdc.rollover` and `rivet cdc --rollover`.
pub const DEFAULT_COUNT: usize = 100_000;

/// A count knob: its name, the code of its zero refusal and the text of that refusal.
#[derive(Clone, Copy)]
pub struct Knob {
    name: &'static str,
    zero: Code,
    zero_text: &'static str,
}

impl Knob {
    pub const CHUNK_SIZE: Knob = Knob {
        name: "chunk_size",
        zero: codes::CONFIG_MODE_SETTING_INVALID,
        zero_text: "{scope}: chunked mode requires {name} >= 1 (got 0)",
    };
    pub const CHUNK_COUNT: Knob = Knob {
        name: "chunk_count",
        zero: codes::CONFIG_CHUNK_COUNT_INVALID,
        zero_text: "{scope}: {name} must be >= 1",
    };
    pub const ROLLOVER: Knob = Knob {
        name: "cdc.rollover",
        zero: codes::CONFIG_CDC_ROLLOVER_INVALID,
        zero_text: "{scope}: {name} must be >= 1 (got 0). A zero rollover rolls a part on every committed transaction; omit the {field} for the default ({default}).",
    };
    pub const PAGE_SIZE: Knob = Knob {
        name: "page_size",
        zero: codes::CONFIG_VALUE_OUT_OF_RANGE,
        zero_text: "{scope}: {name} must be >= 1 (got 0). A zero page reads nothing; omit the {field} to read with a single cursor.",
    };

    /// The largest value a count knob takes: a window or a page past it is not countable in a signed 64-bit key.
    pub const MAX: u64 = i64::MAX as u64;

    fn check(&self, scope: &str, name: &str, field: &str, value: usize) -> Result<()> {
        if value == 0 {
            let text = self
                .zero_text
                .replace("{scope}", scope)
                .replace("{name}", name)
                .replace("{field}", field)
                .replace("{default}", &DEFAULT_COUNT.to_string());
            return Err(coded(self.zero, text));
        }
        if value as u128 > Self::MAX as u128 {
            return Err(coded(
                codes::CONFIG_VALUE_OUT_OF_RANGE,
                format!(
                    "{scope}: {name} {value} is past {}, the largest count a signed 64-bit key holds. Set a value from 1 to {}.",
                    Self::MAX,
                    Self::MAX
                ),
            ));
        }
        Ok(())
    }

    /// Refuse a knob of an export that is zero or past `i64::MAX`.
    pub fn validate_export(&self, export: &str, value: usize) -> Result<()> {
        self.check(&format!("export '{export}'"), self.name, "field", value)
    }

    /// Refuse a Mongo source knob that is zero or past `i64::MAX`.
    pub fn validate_source(&self, value: usize) -> Result<()> {
        self.check("source.mongo", self.name, "field", value)
    }

    /// Refuse a command-line count that is zero or past `i64::MAX`, and return it.
    pub fn cli_count(&self, flag: &str, value: usize) -> Result<usize> {
        self.check("rivet cdc", flag, "flag", value)?;
        Ok(value)
    }

    /// Refuse a null-ratio threshold that is not a number from 0 to 1.
    pub fn ratio(scope: &str, column: &str, value: f64) -> Result<f64> {
        if value.is_nan() || !(0.0..=1.0).contains(&value) {
            return Err(coded(
                codes::CONFIG_VALUE_OUT_OF_RANGE,
                format!(
                    "{scope}: quality.null_ratio_max for column '{column}' is {value}, outside 0 to 1. A ratio is the share of rows that may be NULL; set a number from 0 to 1."
                ),
            ));
        }
        Ok(value)
    }

    /// Refuse the first (by column name) null-ratio threshold that is not a number from 0 to 1.
    pub fn ratios(scope: &str, thresholds: &std::collections::HashMap<String, f64>) -> Result<()> {
        let mut sorted: Vec<_> = thresholds.iter().collect();
        sorted.sort_by(|a, b| a.0.cmp(b.0));
        for (column, value) in sorted {
            Self::ratio(scope, column, *value)?;
        }
        Ok(())
    }

    /// The number of keys `min..=max` holds, 1 for an empty range, in a width no `i64` span can overflow.
    pub fn key_span(min: i64, max: i64) -> u128 {
        (i128::from(max) - i128::from(min)).max(0) as u128 + 1
    }

    /// The keys in `first..=last` as the `i64` an estimate is kept in: 0 for an empty range, `i64::MAX` at most.
    pub fn inclusive_count(first: i64, last: i64) -> i64 {
        if last < first {
            return 0;
        }
        Self::key_span(first, last).min(Self::MAX as u128) as i64
    }

    /// The keys one window holds: `chunk_count` windows over `span` keys, else `chunk_size` keys.
    pub fn window_size(chunk_size: usize, chunk_count: Option<usize>, span: u128) -> u128 {
        match chunk_count {
            Some(count) => span.div_ceil(count.max(1) as u128),
            None => chunk_size as u128,
        }
    }

    /// The distance `min` to `max`, from 1 up to `i64::MAX`, for a sparsity estimate.
    pub fn range_span(min: i64, max: i64) -> i64 {
        (i128::from(max) - i128::from(min)).clamp(1, i128::from(i64::MAX)) as i64
    }

    /// How many windows of `chunk_size` keys cover `range_span`, 0 for a zero size.
    pub fn logical_windows(range_span: i64, chunk_size: usize) -> i64 {
        if chunk_size == 0 {
            return 0;
        }
        (range_span.max(0) as u128)
            .div_ceil(chunk_size as u128)
            .min(Self::MAX as u128) as i64
    }

    /// A count as the `i64` a figure or an estimate is kept in, saturating at `i64::MAX`.
    pub fn saturating_i64(value: usize) -> i64 {
        value.min(Self::MAX as usize) as i64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::error_code;

    fn code_of(r: Result<()>) -> &'static str {
        error_code(&r.expect_err("refused")).expect("coded")
    }

    #[test]
    fn zero_and_past_i64_are_refused_by_the_knobs_own_code() {
        let past = i64::MAX as usize + 1;
        for (knob, zero) in [
            (Knob::CHUNK_SIZE, "RIVET_CONFIG_MODE_SETTING_INVALID"),
            (Knob::CHUNK_COUNT, "RIVET_CONFIG_CHUNK_COUNT_INVALID"),
            (Knob::ROLLOVER, "RIVET_CONFIG_CDC_ROLLOVER_INVALID"),
            (Knob::PAGE_SIZE, "RIVET_CONFIG_VALUE_OUT_OF_RANGE"),
        ] {
            assert_eq!(code_of(knob.validate_export("e", 0)), zero);
            for over in [past, usize::MAX] {
                assert_eq!(
                    code_of(knob.validate_export("e", over)),
                    "RIVET_CONFIG_VALUE_OUT_OF_RANGE"
                );
            }
            for ok in [1, 3, DEFAULT_COUNT, i64::MAX as usize] {
                assert!(knob.validate_export("e", ok).is_ok(), "{ok}");
            }
        }
    }

    #[test]
    fn the_refusal_names_the_knob_the_value_and_the_remedy() {
        let zero = Knob::ROLLOVER.validate_export("orders", 0).unwrap_err();
        assert_eq!(
            zero.to_string(),
            "export 'orders': cdc.rollover must be >= 1 (got 0). A zero rollover rolls a part on every committed transaction; omit the field for the default (100000)."
        );
        let past = Knob::CHUNK_SIZE
            .validate_export("orders", usize::MAX)
            .unwrap_err();
        assert_eq!(
            past.to_string(),
            format!(
                "export 'orders': chunk_size {} is past 9223372036854775807, the largest count a signed 64-bit key holds. Set a value from 1 to 9223372036854775807.",
                usize::MAX
            )
        );
        let flag = Knob::ROLLOVER.cli_count("--rollover", 0).unwrap_err();
        assert_eq!(
            flag.to_string(),
            "rivet cdc: --rollover must be >= 1 (got 0). A zero rollover rolls a part on every committed transaction; omit the flag for the default (100000)."
        );
        assert_eq!(
            Knob::CHUNK_SIZE
                .validate_export("orders", 0)
                .unwrap_err()
                .to_string(),
            "export 'orders': chunked mode requires chunk_size >= 1 (got 0)"
        );
        assert_eq!(
            Knob::CHUNK_COUNT
                .validate_export("orders", 0)
                .unwrap_err()
                .to_string(),
            "export 'orders': chunk_count must be >= 1"
        );
    }

    #[test]
    fn a_source_knob_names_the_source_block() {
        let e = Knob::PAGE_SIZE.validate_source(0).unwrap_err();
        assert_eq!(
            e.to_string(),
            "source.mongo: page_size must be >= 1 (got 0). A zero page reads nothing; omit the field to read with a single cursor."
        );
    }

    #[test]
    fn a_ratio_is_a_number_from_zero_to_one() {
        for ok in [0.0, 0.5, 1.0] {
            assert_eq!(Knob::ratio("export 'e'", "c", ok).unwrap(), ok);
        }
        for bad in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, 1.5, 5.0, -0.1] {
            let e = Knob::ratio("export 'e'", "c", bad).unwrap_err();
            assert_eq!(
                error_code(&e).unwrap(),
                "RIVET_CONFIG_VALUE_OUT_OF_RANGE",
                "{bad}"
            );
            assert!(e.to_string().contains("outside 0 to 1"), "{e}");
        }
    }

    #[test]
    fn a_full_i64_range_has_a_span_and_a_window_no_i64_holds() {
        let span = Knob::key_span(i64::MIN, i64::MAX);
        assert_eq!(span, 1u128 << 64);
        assert_eq!(Knob::window_size(7, Some(1), span), 1u128 << 64);
        assert_eq!(Knob::window_size(7, Some(2), span), 1u128 << 63);
        assert_eq!(Knob::window_size(7, None, span), 7);
        assert_eq!(Knob::key_span(10, 5), 1);
        assert_eq!(Knob::key_span(1, 100), 100);
        assert_eq!(Knob::window_size(7, Some(7), 100), 15);
    }

    #[test]
    fn the_sparsity_figures_saturate_instead_of_overflowing() {
        assert_eq!(Knob::range_span(i64::MIN, i64::MAX), i64::MAX);
        assert_eq!(Knob::range_span(5, 5), 1);
        assert_eq!(Knob::range_span(0, 1000), 1000);
        assert_eq!(Knob::logical_windows(1000, 100), 10);
        assert_eq!(Knob::logical_windows(1001, 100), 11);
        assert_eq!(Knob::logical_windows(i64::MAX, 1), i64::MAX);
        assert_eq!(Knob::logical_windows(1000, 0), 0);
        assert_eq!(Knob::logical_windows(1000, usize::MAX), 1);
        assert_eq!(Knob::saturating_i64(usize::MAX), i64::MAX);
        assert_eq!(Knob::saturating_i64(42), 42);
        assert_eq!(Knob::inclusive_count(i64::MIN, i64::MAX), i64::MAX);
        assert_eq!(Knob::inclusive_count(1, 30), 30);
        assert_eq!(Knob::inclusive_count(10, 5), 0);
    }
}
