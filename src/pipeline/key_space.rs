//! Key-space split points for parallel keyset ranges: values between a key's MIN and MAX, read from no row.

/// The `parts - 1` values that cut `lo..hi` into even steps of the key's own value space; empty when the shape has no such steps.
pub(crate) fn split(lo: &str, hi: &str, parts: usize) -> Vec<String> {
    if parts < 2 {
        return Vec::new();
    }
    ints(lo, hi, parts)
        .or_else(|| floats(lo, hi, parts))
        .or_else(|| uuids(lo, hi, parts))
        .or_else(|| stamps(lo, hi, parts))
        .unwrap_or_else(|| text(lo, hi, parts))
}

/// How a key is read as one ordered integer: its own value, or a uuid's 128 bits without the lowest one.
#[derive(Clone, Copy)]
pub(crate) enum Axis {
    Int,
    Uuid { upper: bool },
}

impl Axis {
    /// The axis a key's text reads on, or `None` for a shape with no integer order or an engine that sorts it differently.
    pub(crate) fn of(key: &str, st: crate::config::SourceType) -> Option<Axis> {
        if key.parse::<i128>().is_ok() {
            return Some(Axis::Int);
        }
        if seeded!(
            "keyset-mssql-uuid-gets-an-axis",
            false,
            st == crate::config::SourceType::Mssql
        ) {
            return None;
        }
        let upper = key.bytes().any(|c| c.is_ascii_uppercase());
        Axis::Uuid { upper }.ord(key).map(|_| Axis::Uuid { upper })
    }

    /// The key's place on the axis.
    pub(crate) fn ord(self, key: &str) -> Option<i128> {
        match self {
            Axis::Int => key.parse().ok(),
            Axis::Uuid { .. } => (key.len() == 36)
                .then(|| uuid::Uuid::try_parse(key).ok())
                .flatten()
                .map(|u| (u.as_u128() >> 1) as i128),
        }
    }

    /// The operator and key text that select every key past a place on the axis.
    pub(crate) fn after(self, at: i128) -> (&'static str, String) {
        match self {
            Axis::Int => (">", self.show(at)),
            Axis::Uuid { .. } => (">=", self.show(at + 1)),
        }
    }

    /// The operator and key text that select every key up to a place on the axis.
    pub(crate) fn upto(self, at: i128) -> (&'static str, String) {
        match self {
            Axis::Int => ("<=", self.show(at)),
            Axis::Uuid { .. } => ("<", self.show(at + 1)),
        }
    }

    /// The operator and key text that select every key below a place on the axis.
    pub(crate) fn below(self, at: i128) -> (&'static str, String) {
        ("<", self.show(at))
    }

    /// The key text for a place on the axis.
    pub(crate) fn show(self, at: i128) -> String {
        match self {
            Axis::Int => at.to_string(),
            Axis::Uuid { upper } => {
                let s = uuid::Uuid::from_u128((at.max(0) as u128) << 1)
                    .hyphenated()
                    .to_string();
                if upper { s.to_uppercase() } else { s }
            }
        }
    }
}

/// The `i`-th of `n` even steps from `lo` across `span`, without overflow.
fn step(lo: u128, span: u128, i: usize, n: usize) -> u128 {
    let (i, n) = (i as u128, n as u128);
    lo.wrapping_add(span / n * i + span % n * i / n)
}

/// Every split point of `a..b` (an ordered pair of 128-bit ordinals), rendered by `show`.
fn steps(a: u128, b: u128, parts: usize, show: impl Fn(u128) -> String) -> Vec<String> {
    (1..parts)
        .map(|i| show(step(a, b.wrapping_sub(a), i, parts)))
        .collect()
}

/// Integer keys (and text that reads as integers in the same order).
fn ints(lo: &str, hi: &str, parts: usize) -> Option<Vec<String>> {
    let (a, b) = (lo.parse::<i128>().ok()?, hi.parse::<i128>().ok()?);
    (a < b).then(|| steps(a as u128, b as u128, parts, |v| (v as i128).to_string()))
}

/// Float keys; a non-finite end has no even steps.
fn floats(lo: &str, hi: &str, parts: usize) -> Option<Vec<String>> {
    let (a, b) = (lo.parse::<f64>().ok()?, hi.parse::<f64>().ok()?);
    if !(a.is_finite() && b.is_finite()) {
        return Some(Vec::new());
    }
    (a < b).then(|| {
        (1..parts)
            .map(|i| {
                let t = i as f64 / parts as f64;
                let v = a + (b - a) * t;
                let v = if v.is_finite() {
                    v
                } else {
                    a * (1.0 - t) + b * t
                };
                v.to_string()
            })
            .collect()
    })
}

/// Hyphenated UUID keys, split as 128-bit numbers in the case `lo` is written in.
fn uuids(lo: &str, hi: &str, parts: usize) -> Option<Vec<String>> {
    let parse = |s: &str| {
        (s.len() == 36)
            .then(|| uuid::Uuid::try_parse(s).ok())
            .flatten()
    };
    let (a, b) = (parse(lo)?.as_u128(), parse(hi)?.as_u128());
    let upper = lo.bytes().any(|c| c.is_ascii_uppercase());
    (a < b).then(|| {
        steps(a, b, parts, |v| {
            let s = uuid::Uuid::from_u128(v).hyphenated().to_string();
            if upper { s.to_uppercase() } else { s }
        })
    })
}

/// How a date or timestamp key is written: the date-time separator, the fraction digits, whether the fraction is all zeros, and a zone suffix.
struct Stamp {
    sep: Option<char>,
    frac: usize,
    whole: bool,
    zone: &'static str,
}

/// A date (days) or timestamp (microseconds) key and how it is written.
fn stamp(s: &str) -> Option<(i128, Stamp)> {
    let (body, zone) = s.strip_suffix("+00").map_or((s, ""), |b| (b, "+00"));
    if body.len() == 10 {
        let d = chrono::NaiveDate::parse_from_str(body, "%Y-%m-%d").ok()?;
        let st = Stamp {
            sep: None,
            frac: 0,
            whole: true,
            zone,
        };
        return Some((i128::from(chrono::Datelike::num_days_from_ce(&d)), st));
    }
    let sep = body.chars().nth(10).filter(|c| *c == ' ' || *c == 'T')?;
    let dt =
        chrono::NaiveDateTime::parse_from_str(&body.replacen(sep, " ", 1), "%Y-%m-%d %H:%M:%S%.f")
            .ok()?;
    let fraction = body.split_once('.').map_or("", |(_, f)| f);
    let whole = fraction.bytes().all(|c| c == b'0');
    let st = Stamp {
        sep: Some(sep),
        frac: fraction.len(),
        whole,
        zone,
    };
    Some((i128::from(dt.and_utc().timestamp_micros()), st))
}

/// The key written as `st` for a date (days) or timestamp (microseconds) ordinal.
fn show_stamp(v: i128, st: &Stamp) -> Option<String> {
    let Some(sep) = st.sep else {
        let d = chrono::NaiveDate::from_num_days_from_ce_opt(i32::try_from(v).ok()?)?;
        return Some(format!("{}{}", d.format("%Y-%m-%d"), st.zone));
    };
    let v = if st.whole {
        v - v.rem_euclid(1_000_000)
    } else {
        v
    };
    let dt = chrono::DateTime::from_timestamp_micros(i64::try_from(v).ok()?)?.naive_utc();
    let nanos = format!("{:09}", chrono::Timelike::nanosecond(&dt));
    let frac = match st.frac {
        0 => String::new(),
        n => format!(".{}", nanos.get(..n).unwrap_or(&nanos)),
    };
    Some(format!(
        "{}{sep}{}{frac}{}",
        dt.format("%Y-%m-%d"),
        dt.format("%H:%M:%S"),
        st.zone
    ))
}

/// Date and timestamp keys, written as the source wrote `lo` with the finer of the two fractions, in whole seconds when both fractions are zero.
fn stamps(lo: &str, hi: &str, parts: usize) -> Option<Vec<String>> {
    let ((a, sa), (b, sb)) = (stamp(lo)?, stamp(hi)?);
    let same = sa.sep == sb.sep && sa.zone == sb.zone;
    let st = Stamp {
        frac: sa.frac.max(sb.frac),
        whole: sa.whole && sb.whole,
        ..sa
    };
    (same && a < b).then(|| {
        (1..parts)
            .filter_map(|i| show_stamp(step(a as u128, (b - a) as u128, i, parts) as i128, &st))
            .collect()
    })
}

/// The digit a character counts as in [`text`]: 0 ends the string, control characters count as a space and non-ASCII as one digit above `~`.
fn digit(c: char) -> u128 {
    u128::from((c as u32).clamp(0x20, 0x7F) - 0x1F)
}

/// The printable ASCII character a [`text`] digit is written as.
fn glyph(d: u128) -> char {
    char::from(d.min(0x5F) as u8 + 0x1F)
}

/// Text keys: the characters after the common prefix read as digits of one number, split, and written back in printable ASCII.
fn text(lo: &str, hi: &str, parts: usize) -> Vec<String> {
    let prefix: String = lo
        .chars()
        .zip(hi.chars())
        .take_while(|(a, b)| a == b)
        .map(|(a, _)| a)
        .collect();
    let n = prefix.chars().count();
    let (rl, rh): (Vec<char>, Vec<char>) =
        (lo.chars().skip(n).collect(), hi.chars().skip(n).collect());
    let base = rl.iter().chain(&rh).map(|c| digit(*c)).max().unwrap_or(1) + 1;
    let mut width = 0;
    let mut cap: u128 = 1;
    while let Some(next) = cap
        .checked_mul(base)
        .filter(|_| width <= rl.len().max(rh.len()))
    {
        cap = next;
        width += 1;
    }
    let num = |r: &[char]| {
        (0..width).fold(0u128, |acc, j| {
            acc * base + r.get(j).map_or(0, |c| digit(*c))
        })
    };
    let (a, b) = (num(&rl).min(num(&rh)), num(&rl).max(num(&rh)));
    if a == b {
        return Vec::new();
    }
    steps(a, b, parts, |mut v| {
        let mut ds = vec![0u128; width];
        for d in ds.iter_mut().rev() {
            *d = v % base;
            v /= base;
        }
        let tail: String = ds
            .iter()
            .take_while(|d| **d != 0)
            .map(|d| glyph(*d))
            .collect();
        format!("{prefix}{tail}")
    })
}

#[cfg(test)]
mod tests {
    use super::{Axis, split};

    const ST: crate::config::SourceType = crate::config::SourceType::Postgres;

    /// Owned strings for a comparison.
    fn v(xs: &[&str]) -> Vec<String> {
        xs.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn integers_split_evenly_across_the_whole_i128_range() {
        assert_eq!(split("0", "1000", 4), v(&["250", "500", "750"]));
        assert_eq!(split("-10", "10", 2), v(&["0"]));
        assert_eq!(split("1", "3", 4), v(&["1", "2", "2"]));
        let wide = split(&i128::MIN.to_string(), &i128::MAX.to_string(), 2);
        assert_eq!(wide, v(&["-1"]));
        assert_eq!(split("5", "5", 4), Vec::<String>::new());
        assert_eq!(split("1", "100", 1), Vec::<String>::new());
    }

    #[test]
    fn floats_split_evenly_and_a_non_finite_end_does_not() {
        assert_eq!(split("0.5", "2.5", 4), v(&["1", "1.5", "2"]));
        assert_eq!(split("-1e308", "1e308", 2), v(&["0"]));
        assert!(split("1.5", "inf", 4).is_empty());
        assert!(split("1.5", "NaN", 4).is_empty());
    }

    #[test]
    fn uuids_split_as_numbers_in_the_case_they_are_written_in() {
        let got = split(
            "00000000-0000-7000-8000-000000000000",
            "40000000-0000-7000-8000-000000000000",
            4,
        );
        assert_eq!(
            got,
            v(&[
                "10000000-0000-7000-8000-000000000000",
                "20000000-0000-7000-8000-000000000000",
                "30000000-0000-7000-8000-000000000000",
            ])
        );
        let up = split(
            "0000000A-0000-0000-0000-000000000000",
            "0000000C-0000-0000-0000-000000000000",
            2,
        );
        assert_eq!(up, v(&["0000000B-0000-0000-0000-000000000000"]));
    }

    #[test]
    fn stamps_keep_the_way_the_source_wrote_them() {
        assert_eq!(
            split("2024-01-01", "2024-01-05", 4),
            v(&["2024-01-02", "2024-01-03", "2024-01-04"])
        );
        assert_eq!(
            split("2024-01-01 00:00:00", "2024-01-01 00:00:04", 4),
            v(&[
                "2024-01-01 00:00:01",
                "2024-01-01 00:00:02",
                "2024-01-01 00:00:03"
            ])
        );
        assert_eq!(
            split(
                "2024-01-01T00:00:00.000000",
                "2024-01-01T00:00:04.000000",
                4
            ),
            v(&[
                "2024-01-01T00:00:01.000000",
                "2024-01-01T00:00:02.000000",
                "2024-01-01T00:00:03.000000"
            ])
        );
        assert_eq!(
            split(
                "2024-01-01T00:00:00.000000",
                "2024-01-01T00:00:01.000001",
                2
            ),
            v(&["2024-01-01T00:00:00.500000"])
        );
        assert_eq!(
            split("2024-01-01 00:00:00+00", "2024-01-01 00:00:00.2+00", 2),
            v(&["2024-01-01 00:00:00.1+00"])
        );
        assert_eq!(split("0001-01-01", "9999-12-31", 2), v(&["5000-07-02"]));
    }

    #[test]
    fn text_splits_after_the_common_prefix_into_printable_characters() {
        assert_eq!(split("a", "c", 2), v(&["b"]));
        assert_eq!(split("user_00", "user_99", 3), v(&["user_33", "user_66"]));
        for c in split("k\u{1}", "k\u{10FFFF}", 8) {
            let tail = c.strip_prefix('k').unwrap_or_else(|| panic!("{c:?}"));
            assert!(tail.chars().all(|c| (' '..='~').contains(&c)), "{c:?}");
        }
        assert_eq!(split("é", "ü", 2), Vec::<String>::new());
        assert!(split("same", "same", 4).is_empty());
        assert_eq!(split("zeta", "alpha", 2).len(), 1);
    }

    #[test]
    fn an_axis_reads_integers_and_uuids_in_their_own_order_and_case() {
        let lo = "00000000-0000-0000-0000-000000000001";
        let hi = "FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFE";
        let up = Axis::of(hi, ST).unwrap();
        assert!(matches!(up, Axis::Uuid { upper: true }));
        let (a, b) = (up.ord(lo).unwrap(), up.ord(hi).unwrap());
        assert!(0 <= a && a < b);
        assert_eq!(up.show(b), hi);
        let mid = up.show(a + (b - a) / 2);
        assert!(lo < mid.as_str() && mid.as_str() < hi, "{mid}");
        assert!(matches!(Axis::of("-42", ST), Some(Axis::Int)));
        assert_eq!(Axis::Int.show(Axis::Int.ord("-42").unwrap()), "-42");
        assert!(Axis::of("abc", ST).is_none());
        assert!(Axis::of("0123456789abcdef0123456789abcdef", ST).is_none());
    }

    #[test]
    fn a_uuid_key_has_no_axis_on_sql_server_whose_order_differs() {
        let u = "00000000-0000-0000-0000-000000000001";
        assert!(Axis::of(u, crate::config::SourceType::Mssql).is_none());
        assert!(Axis::of(u, crate::config::SourceType::Postgres).is_some());
        assert!(matches!(
            Axis::of("7", crate::config::SourceType::Mssql),
            Some(Axis::Int)
        ));
    }

    #[test]
    fn a_uuid_place_selects_every_key_of_its_pair_on_both_sides() {
        let odd = "00000000-0000-0000-0000-000000000003";
        let axis = Axis::of(odd, ST).unwrap();
        let at = axis.ord(odd).unwrap();
        let (op, past) = axis.after(at);
        assert_eq!(op, ">=");
        assert!(odd < past.as_str(), "{past}");
        let (op, upto) = axis.upto(at);
        assert_eq!(op, "<");
        assert!(odd < upto.as_str(), "{upto}");
        assert_eq!(Axis::Int.after(5), (">", "5".to_string()));
        assert_eq!(Axis::Int.upto(5), ("<=", "5".to_string()));
    }

    #[test]
    fn a_place_below_the_nil_uuid_shows_as_the_nil_uuid() {
        let nil = "00000000-0000-0000-0000-000000000000";
        let axis = Axis::of(nil, ST).unwrap();
        assert_eq!(axis.ord(nil), Some(0));
        assert_eq!(axis.show(-1), nil);
    }

    #[test]
    fn every_split_point_lies_between_the_ends_of_an_ordered_pair() {
        for (lo, hi) in [
            ("-7", "1000000007"),
            ("abc", "abx"),
            ("2001-02-03", "2001-12-31"),
        ] {
            for c in split(lo, hi, 16) {
                assert!(lo <= c.as_str() || lo.starts_with('-'), "{lo} {c}");
                assert!(c.as_str() <= hi || lo.starts_with('-'), "{c} {hi}");
            }
        }
    }
}
