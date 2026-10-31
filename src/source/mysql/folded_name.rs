//! Name comparison for a server that folds identifier case (`lower_case_table_names` other than 0).

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

/// Whether `name` is `text`, or a part of it when `inside`, folding ASCII case and letting one non-ASCII character stand for any one letter.
fn folds_to(name: &str, text: &[u8], inside: bool) -> bool {
    let mut reach = vec![inside; text.len() + 1];
    reach[0] = true;
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
    if inside {
        reach.iter().any(|r| *r)
    } else {
        reach[text.len()]
    }
}

/// Whether `text` names `name` as the server resolves it: exactly when it keeps case, else under the fold above.
pub(crate) fn names_same(fold: bool, name: &str, text: &str) -> bool {
    if fold {
        folds_to(name, text.as_bytes(), false)
    } else {
        name == text
    }
}

/// Whether the bytes `raw` contain `name`: in any ASCII case when the server keeps case, else under the fold above.
pub(crate) fn mentions_name(fold: bool, name: &str, raw: &[u8]) -> bool {
    if name.is_empty() {
        return false;
    }
    if fold && !raw.is_ascii() {
        folds_to(name, raw, true)
    } else {
        raw.windows(name.len())
            .any(|w| w.eq_ignore_ascii_case(name.as_bytes()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const KELVIN: &str = "\u{212a}";
    const DOTTED_I: &str = "\u{130}";

    /// The Kelvin sign and the dotted capital I stand for `k` and `i` only on a server that folds.
    #[test]
    fn a_non_ascii_character_stands_for_a_letter_only_when_the_server_folds() {
        let kinds = format!("{KELVIN}inds");
        assert!(names_same(true, "kinds", &kinds));
        assert!(!names_same(false, "kinds", &kinds));
        let ship = format!("sh{DOTTED_I}p");
        assert!(names_same(true, "ship", &ship));
        assert!(!names_same(false, "ship", &ship));
    }

    /// ASCII folds in either direction under the fold and not without it; the length must agree.
    #[test]
    fn ascii_case_folds_only_when_the_server_folds() {
        assert!(names_same(true, "kinds", "KINDS"));
        assert!(!names_same(false, "kinds", "KINDS"));
        assert!(names_same(false, "kinds", "kinds"));
        assert!(!names_same(true, "kinds", "kind"));
        assert!(!names_same(true, "kinds", "kindss"));
        assert!(!names_same(true, "kinds", ""));
    }

    /// A double-byte character whose second byte is ASCII (GBK `0x81 0x60`) is one letter.
    #[test]
    fn a_double_byte_character_with_an_ascii_trail_is_one_letter() {
        let text = [b'x', 0x81, 0x60, b'y'];
        assert!(folds_to("xky", &text, false));
        assert!(!folds_to("xy", &text, false));
        assert!(!folds_to("xkky", &text, false));
        assert!(folds_to("ky", &text, true));
    }

    /// A four-byte character (GB18030: lead, digit, lead, digit) is one letter.
    #[test]
    fn a_four_byte_character_is_one_letter() {
        let text = [0x81, 0x30, 0x81, 0x30, b'a'];
        assert!(folds_to("ka", &text, false));
        assert!(!folds_to("a", &text, false));
    }

    /// A mention is found inside a statement, across a character that folds, and not when no run of letters can stand for the name.
    #[test]
    fn a_mention_is_found_inside_a_statement() {
        let stmt = format!("TRUNCATE TABLE shop.`{KELVIN}inds`");
        assert!(mentions_name(true, "kinds", stmt.as_bytes()));
        assert!(!mentions_name(false, "kinds", stmt.as_bytes()));
        assert!(mentions_name(false, "kinds", b"truncate table shop.KINDS"));
        assert!(mentions_name(true, "kinds", b"truncate table shop.KINDS"));
        assert!(!mentions_name(true, "kinds", b"truncate table shop.other"));
        assert!(!mentions_name(true, "", b"anything"));
        let short = format!("ALTER TABLE {KELVIN}ind");
        assert!(!mentions_name(true, "kinds", short.as_bytes()));
    }
}
