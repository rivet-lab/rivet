//! PostgreSQL binary `numeric` wire format decoder (`numeric.c::numeric_recv`).

use std::io::Cursor;

use bigdecimal::num_bigint::{BigInt, Sign};
use bigdecimal::{BigDecimal, Zero};
use byteorder::{BigEndian, ReadBytesExt};
use postgres_types::{FromSql, Type};

const NUMERIC_POS: u16 = 0x0000;
const NUMERIC_NEG: u16 = 0x4000;
const NUMERIC_NAN: u16 = 0xC000;
const NUMERIC_PINF: u16 = 0xD000;
const NUMERIC_NINF: u16 = 0xF000;
const PG_NBASE: i64 = 10_000;

/// Borrowed Postgres `numeric` OID payload (`Type::NUMERIC`).
#[derive(Debug, Clone, Copy)]
pub struct PgNumericWire<'a>(pub &'a [u8]);

impl<'a> FromSql<'a> for PgNumericWire<'a> {
    fn from_sql(
        ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        if ty != &Type::NUMERIC {
            return Err(format!("expected NUMERIC oid, got {ty}",).into());
        }
        Ok(PgNumericWire(raw))
    }

    fn accepts(ty: &Type) -> bool {
        ty == &Type::NUMERIC
    }
}

/// PostgreSQL's own text for a non-finite `numeric`, or `None` for a finite one (and for a payload too short to carry a sign field).
pub fn numeric_wire_special_text(wire: &[u8]) -> Option<&'static str> {
    let mut cur = Cursor::new(wire);
    let _ndigits = cur.read_u16::<BigEndian>().ok()?;
    let _weight = cur.read_i16::<BigEndian>().ok()?;
    match cur.read_u16::<BigEndian>().ok()? {
        NUMERIC_NAN => Some("NaN"),
        NUMERIC_PINF => Some("Infinity"),
        NUMERIC_NINF => Some("-Infinity"),
        _ => None,
    }
}

/// Plain decimal text at the value's display scale, the digits PostgreSQL's own output function prints.
pub fn numeric_wire_normalized_plain(wire: &[u8]) -> Option<String> {
    let display_scale = i64::from(Cursor::new(wire.get(6..8)?).read_u16::<BigEndian>().ok()?);
    let bd = wire_to_big_decimal(wire)?.normalized();
    let natural_scale = bd.as_bigint_and_exponent().1;
    let scale = if seeded!("pg-numeric-text-drops-display-scale", true, false) {
        natural_scale
    } else {
        natural_scale.max(display_scale)
    };

    Some(bd.with_scale(scale).to_plain_string().trim().to_string())
}

pub(crate) fn wire_to_big_decimal(wire: &[u8]) -> Option<BigDecimal> {
    let mut cur = Cursor::new(wire);
    let ndigits = cur.read_u16::<BigEndian>().ok()? as usize;
    let weight = cur.read_i16::<BigEndian>().ok()?;
    let sign_field = cur.read_u16::<BigEndian>().ok()?;
    let _display_scale = cur.read_u16::<BigEndian>().ok()?;

    if wire.len().checked_sub(8)? < ndigits.saturating_mul(2) {
        return None;
    }

    match sign_field {
        NUMERIC_POS | NUMERIC_NEG => {}
        NUMERIC_NAN | NUMERIC_PINF | NUMERIC_NINF => return None,
        _ => return None,
    };

    let sign = if sign_field == NUMERIC_NEG {
        Sign::Minus
    } else {
        Sign::Plus
    };

    if ndigits == 0 {
        return Some(BigDecimal::zero());
    }

    let mut limbs = Vec::<i16>::with_capacity(ndigits);
    for _ in 0..ndigits {
        let d = cur.read_i16::<BigEndian>().ok()?;
        if !(0..PG_NBASE).contains(&(d as i64)) {
            return None;
        }
        limbs.push(d);
    }

    let scale = ((limbs.len() as i64) - i64::from(weight) - 1) * 4;
    let mut cents = Vec::<u8>::with_capacity(limbs.len().saturating_mul(2));
    for limb in limbs {
        cents.push((limb / 100) as u8);
        cents.push((limb % 100) as u8);
    }

    let coef = BigInt::from_radix_be(sign, &cents, 100)?;

    Some(BigDecimal::new(coef, scale))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_wire_reader_takes_numeric_and_no_other_type() {
        assert!(<PgNumericWire as FromSql>::accepts(&Type::NUMERIC));
        assert!(!<PgNumericWire as FromSql>::accepts(&Type::INT8));
        let raw = [0u8, 0, 0, 0, 0, 0, 0, 0];
        assert_eq!(
            PgNumericWire::from_sql(&Type::NUMERIC, &raw).unwrap().0,
            raw
        );
        assert!(PgNumericWire::from_sql(&Type::TEXT, &raw).is_err());
    }

    /// Build a raw PG numeric wire payload from its constituent fields.
    fn encode(ndigits: u16, weight: i16, sign: u16, dscale: u16, digits: &[i16]) -> Vec<u8> {
        let mut buf = Vec::with_capacity(8 + digits.len() * 2);
        buf.extend_from_slice(&ndigits.to_be_bytes());
        buf.extend_from_slice(&weight.to_be_bytes());
        buf.extend_from_slice(&sign.to_be_bytes());
        buf.extend_from_slice(&dscale.to_be_bytes());
        for &d in digits {
            buf.extend_from_slice(&d.to_be_bytes());
        }
        buf
    }

    #[test]
    fn zero_ndigits_gives_zero() {
        let wire = encode(0, 0, NUMERIC_POS, 0, &[]);
        assert_eq!(numeric_wire_normalized_plain(&wire).as_deref(), Some("0"));
    }

    #[test]
    fn value_one() {
        let wire = encode(1, 0, NUMERIC_POS, 0, &[1]);
        assert_eq!(numeric_wire_normalized_plain(&wire).as_deref(), Some("1"));
    }

    #[test]
    fn value_one_hundred() {
        let wire = encode(1, 0, NUMERIC_POS, 0, &[100]);
        assert_eq!(numeric_wire_normalized_plain(&wire).as_deref(), Some("100"));
    }

    #[test]
    fn value_9999_single_limb() {
        let wire = encode(1, 0, NUMERIC_POS, 0, &[9999]);
        assert_eq!(
            numeric_wire_normalized_plain(&wire).as_deref(),
            Some("9999")
        );
    }

    #[test]
    fn value_10000_two_limbs() {
        let wire = encode(2, 1, NUMERIC_POS, 0, &[1, 0]);
        assert_eq!(
            numeric_wire_normalized_plain(&wire).as_deref(),
            Some("10000")
        );
    }

    #[test]
    fn negative_five() {
        let wire = encode(1, 0, NUMERIC_NEG, 0, &[5]);
        assert_eq!(numeric_wire_normalized_plain(&wire).as_deref(), Some("-5"));
    }

    #[test]
    fn negative_large() {
        let wire = encode(2, 1, NUMERIC_NEG, 0, &[1, 2345]);
        assert_eq!(
            numeric_wire_normalized_plain(&wire).as_deref(),
            Some("-12345")
        );
    }

    #[test]
    fn value_point_01() {
        let wire = encode(1, -1, NUMERIC_POS, 2, &[100]);
        assert_eq!(
            numeric_wire_normalized_plain(&wire).as_deref(),
            Some("0.01")
        );
    }

    #[test]
    fn value_12_dot_34() {
        let wire = encode(2, 0, NUMERIC_POS, 2, &[12, 3400]);
        assert_eq!(
            numeric_wire_normalized_plain(&wire).as_deref(),
            Some("12.34")
        );
    }

    #[test]
    fn negative_fractional() {
        let wire = encode(2, 0, NUMERIC_NEG, 2, &[12, 3400]);
        assert_eq!(
            numeric_wire_normalized_plain(&wire).as_deref(),
            Some("-12.34")
        );
    }

    #[test]
    fn display_scale_pads_trailing_zeros_like_the_servers_text() {
        for (wire, want) in [
            (encode(2, 0, NUMERIC_POS, 2, &[1, 5000]), "1.50"),
            (encode(1, -1, NUMERIC_POS, 2, &[1000]), "0.10"),
            (encode(0, 0, NUMERIC_POS, 3, &[]), "0.000"),
            (encode(1, 0, NUMERIC_NEG, 3, &[1]), "-1.000"),
            (encode(1, 0, NUMERIC_POS, 3, &[1000]), "1000.000"),
            (encode(2, 0, NUMERIC_POS, 10, &[2, 5000]), "2.5000000000"),
            (encode(1, 1, NUMERIC_POS, 0, &[1]), "10000"),
        ] {
            assert_eq!(numeric_wire_normalized_plain(&wire).as_deref(), Some(want));
        }
    }

    #[test]
    fn a_display_scale_below_the_digits_never_drops_a_digit() {
        let wire = encode(2, 0, NUMERIC_POS, 0, &[1, 2500]);
        assert_eq!(
            numeric_wire_normalized_plain(&wire).as_deref(),
            Some("1.25")
        );
    }

    #[test]
    fn a_header_without_a_display_scale_returns_none() {
        assert!(numeric_wire_normalized_plain(&[0, 0, 0, 0, 0, 0, 0]).is_none());
    }

    #[test]
    fn nan_returns_none() {
        let wire = encode(0, 0, NUMERIC_NAN, 0, &[]);
        assert!(numeric_wire_normalized_plain(&wire).is_none());
    }

    #[test]
    fn positive_infinity_returns_none() {
        let wire = encode(0, 0, NUMERIC_PINF, 0, &[]);
        assert!(numeric_wire_normalized_plain(&wire).is_none());
    }

    #[test]
    fn negative_infinity_returns_none() {
        let wire = encode(0, 0, NUMERIC_NINF, 0, &[]);
        assert!(numeric_wire_normalized_plain(&wire).is_none());
    }

    #[test]
    fn unknown_sign_field_returns_none() {
        let wire = encode(0, 0, 0xBEEF, 0, &[]);
        assert!(numeric_wire_normalized_plain(&wire).is_none());
    }

    #[test]
    fn empty_slice_returns_none() {
        assert!(numeric_wire_normalized_plain(&[]).is_none());
    }

    #[test]
    fn truncated_header_returns_none() {
        assert!(numeric_wire_normalized_plain(&[0, 1, 0, 0, 0, 0]).is_none());
    }

    #[test]
    fn ndigits_exceeds_actual_data_returns_none() {
        let wire = encode(3, 0, NUMERIC_POS, 0, &[]);
        assert!(numeric_wire_normalized_plain(&wire).is_none());
    }

    #[test]
    fn limb_out_of_range_returns_none() {
        let wire = encode(1, 0, NUMERIC_POS, 0, &[10000]);
        assert!(numeric_wire_normalized_plain(&wire).is_none());
    }
}

#[cfg(test)]
mod renderer_twins {
    use super::numeric_wire_normalized_plain;
    use crate::types::decimal_plain;

    /// The PG `numeric` wire payload for `unscaled * 10^-scale` with display scale `scale`.
    fn wire(unscaled: i128, scale: u16) -> Vec<u8> {
        let digits = unscaled.unsigned_abs().to_string();
        let s = usize::from(scale);
        let padded = format!("{digits:0>w$}", w = s + 1);
        let (int, frac) = padded.split_at(padded.len() - s);
        let int = format!("{int:0>w$}", w = int.len().div_ceil(4) * 4);
        let frac = format!("{frac:0<w$}", w = frac.len().div_ceil(4) * 4);
        let limbs: Vec<i16> = format!("{int}{frac}")
            .as_bytes()
            .chunks(4)
            .map(|c| std::str::from_utf8(c).unwrap().parse().unwrap())
            .collect();
        let mut out = Vec::new();
        out.extend_from_slice(&(limbs.len() as u16).to_be_bytes());
        out.extend_from_slice(&((int.len() / 4) as i16 - 1).to_be_bytes());
        out.extend_from_slice(&(if unscaled < 0 { 0x4000u16 } else { 0 }).to_be_bytes());
        out.extend_from_slice(&scale.to_be_bytes());
        for l in limbs {
            out.extend_from_slice(&l.to_be_bytes());
        }
        out
    }

    #[test]
    fn normalized_plain_matches_decimal_plain_without_trailing_zeros() {
        for (u, s) in [
            (12345, 2),
            (-12345, 2),
            (5, 3),
            (-5, 3),
            (42, 0),
            (-42, 0),
            (0, 0),
            (-12_000, 0),
            (99_999_999_999_999_999_999_999_999_999_999_999_999, 38),
            (i128::MIN + 1, 0),
        ] {
            assert_eq!(
                numeric_wire_normalized_plain(&wire(u, s)).as_deref(),
                Some(decimal_plain(u, s as i8).as_str()),
                "({u}, {s})"
            );
        }
    }

    #[test]
    fn normalized_plain_keeps_every_scale_like_decimal_plain() {
        for s in 0..=24u16 {
            for u in [0i128, 1, -1, 150, -1_000, 123_456_789, i128::from(i64::MAX)] {
                assert_eq!(
                    numeric_wire_normalized_plain(&wire(u, s)).as_deref(),
                    Some(decimal_plain(u, s as i8).as_str()),
                    "({u}, {s})"
                );
            }
        }
    }

    #[test]
    fn normalized_plain_keeps_the_scale_like_decimal_plain() {
        for (u, s) in [(150, 2), (0, 3), (-1_000, 3)] {
            assert_eq!(
                numeric_wire_normalized_plain(&wire(u, s)).as_deref(),
                Some(decimal_plain(u, s as i8).as_str()),
                "({u}, {s})"
            );
        }
    }
}
