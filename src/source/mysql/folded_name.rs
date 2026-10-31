//! Name search for a server that folds identifier case (`lower_case_table_names` other than 0).

/// Whether a byte after a non-ASCII byte can be the rest of that character in a multibyte client character set.
fn is_trail(b: u8) -> bool {
    matches!(b, 0x30..=0x39 | 0x40..=0x7e)
}

/// Positions where a single non-ASCII character starting at `at` can end.
fn character_ends(text: &[u8], at: usize) -> Vec<usize> {
    let mut ends = Vec::new();
    let mut frontier = vec![at];
    for _ in 0..4 {
        let mut next = Vec::new();
        for &p in &frontier {
            if text.get(p).is_none_or(|b| *b < 0x80) {
                continue;
            }
            next.push(p + 1);
            if text.get(p + 1).copied().is_some_and(is_trail) {
                next.push(p + 2);
            }
        }
        next.sort_unstable();
        next.dedup();
        ends.extend(&next);
        frontier = next;
    }
    ends.sort_unstable();
    ends.dedup();
    ends
}

/// Whether `name` is a part of `text`, folding ASCII case and letting one non-ASCII character stand for any one letter.
fn folds_inside(name: &str, text: &[u8]) -> bool {
    let mut reach = vec![true; text.len() + 1];
    for c in name.bytes() {
        let mut next = vec![false; text.len() + 1];
        for at in (0..text.len()).filter(|&at| reach[at]) {
            let b = text[at];
            if b < 0x80 {
                if b.eq_ignore_ascii_case(&c) {
                    next[at + 1] = true;
                }
            } else {
                for end in character_ends(text, at) {
                    next[end] = true;
                }
            }
        }
        reach = next;
    }
    reach.iter().any(|r| *r)
}

/// Whether the bytes `raw` contain `name`: in any ASCII case when the server keeps case, else under the fold above.
pub(crate) fn mentions_name(fold: bool, name: &str, raw: &[u8]) -> bool {
    if name.is_empty() {
        return false;
    }
    if fold && !raw.is_ascii() {
        folds_inside(name, raw)
    } else {
        raw.windows(name.len())
            .any(|w| w.eq_ignore_ascii_case(name.as_bytes()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const KELVIN: &str = "\u{212a}";

    /// The Kelvin sign stands for `k` only on a server that folds.
    #[test]
    fn a_non_ascii_character_stands_for_a_letter_only_when_the_server_folds() {
        let stmt = format!("TRUNCATE TABLE shop.`{KELVIN}inds`");
        assert!(mentions_name(true, "kinds", stmt.as_bytes()));
        assert!(!mentions_name(false, "kinds", stmt.as_bytes()));
    }

    /// ASCII case folds either way and the length must agree.
    #[test]
    fn ascii_case_is_ignored_and_the_length_must_agree() {
        assert!(mentions_name(false, "kinds", b"truncate table shop.KINDS"));
        assert!(mentions_name(true, "kinds", b"truncate table shop.KINDS"));
        assert!(!mentions_name(true, "kinds", b"truncate table shop.kind"));
        assert!(!mentions_name(true, "kinds", b"truncate table shop.other"));
        assert!(!mentions_name(true, "", b"anything"));
    }

    /// A double-byte character whose second byte is ASCII (GBK `0x81 0x60`) is one letter.
    #[test]
    fn a_double_byte_character_with_an_ascii_trail_is_one_letter() {
        let text = [b'x', 0x81, 0x60, b'y'];
        assert!(folds_inside("xky", &text));
        assert!(!folds_inside("xkky", &text));
        assert!(folds_inside("ky", &text));
    }

    /// A four-byte character (GB18030: lead, digit, lead, digit) is one letter.
    #[test]
    fn a_four_byte_character_is_one_letter() {
        let text = [0x81, 0x30, 0x90, 0x32, b'a'];
        assert!(folds_inside("ka", &text));
        assert!(!folds_inside("kkka", &text));
    }

    /// A three-byte character (ujis `0x8F 0xAA 0xC4`) is one letter.
    #[test]
    fn a_three_byte_character_is_one_letter() {
        let text = [b's', b'h', 0x8F, 0xAA, 0xC4, b'p'];
        assert!(folds_inside("ship", &text));
    }

    /// A run that is too short for the name is no mention.
    #[test]
    fn a_run_too_short_for_the_name_is_no_mention() {
        let short = format!("ALTER TABLE {KELVIN}ind");
        assert!(!mentions_name(true, "kinds", short.as_bytes()));
    }
}
