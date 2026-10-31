//! Name search for a server that folds identifier case (`lower_case_table_names` other than 0).

/// Whether a byte after a non-ASCII byte can be the rest of that character in a multibyte client character set.
fn is_trail(b: u8) -> bool {
    matches!(b, 0x30..=0x39 | 0x40..=0x7e)
}

/// What the server folds to an ASCII letter: code point and letter, the `folds` of the statement-truth corpus header on 8.0.46 and 8.4.13.
pub(crate) const FOLDED_TO_ASCII: [(u32, u8); 2] = [(0x0130, b'i'), (0x212A, b'k')];

/// Whether a non-ASCII character can stand for the letter `c`.
fn folded_from_non_ascii(c: u8) -> bool {
    FOLDED_TO_ASCII
        .iter()
        .any(|&(_, letter)| letter.eq_ignore_ascii_case(&c))
}

/// Whether `name` (at most 127 bytes, as a MySQL identifier is) is a part of `text`, folding ASCII case and letting one non-ASCII character stand for the letters the server folds, in one pass over `text`.
fn folds_inside(name: &str, text: &[u8]) -> bool {
    let mut masks = [0u128; 128];
    let mut stands = 0u128;
    for (j, c) in name.bytes().enumerate() {
        masks[usize::from(c.to_ascii_lowercase())] |= 1 << j;
        masks[usize::from(c.to_ascii_uppercase())] |= 1 << j;
        if folded_from_non_ascii(c) {
            stands |= 1 << j;
        }
    }
    let goal = 1u128 << name.len();
    let (mut matched, mut whole, mut lead) = (1u128, [0u128; 4], [0u128; 4]);
    for &x in text {
        let mut next = 1u128;
        let (mut next_whole, mut next_lead) = ([0u128; 4], [0u128; 4]);
        if x < 0x80 {
            next |= (matched & masks[usize::from(x)]) << 1;
            if is_trail(x) {
                next_whole = lead;
            }
        } else {
            next_whole[0] = matched & stands;
            next_lead[0] = next_whole[0];
            next_whole[1..].copy_from_slice(&whole[..3]);
            next_lead[1..].copy_from_slice(&whole[..3]);
        }
        for w in next_whole {
            next |= w << 1;
        }
        if next & goal != 0 {
            return true;
        }
        (matched, whole, lead) = (next, next_whole, next_lead);
    }
    false
}

/// Whether the bytes `raw` contain `name`: in any ASCII case when the server keeps case, else under the fold above (a name without `i` or `k`, or longer than a MySQL identifier, has no non-ASCII spelling).
pub(crate) fn mentions_name(fold: bool, name: &str, raw: &[u8]) -> bool {
    if name.is_empty() {
        return false;
    }
    if fold && !raw.is_ascii() && name.len() < 128 && name.bytes().any(folded_from_non_ascii) {
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

    /// A non-ASCII unit stands only for a letter the server folds to.
    #[test]
    fn a_non_ascii_unit_stands_only_for_the_letters_the_server_folds() {
        assert!(!mentions_name(true, "orders", "一二三四五六".as_bytes()));
        assert!(!mentions_name(
            true,
            "orders",
            "\u{446}\u{435}\u{43d}\u{44b}".repeat(4).as_bytes()
        ));
        assert!(!mentions_name(
            true,
            "orders",
            "ALTER TABLE x COMMENT='一二三四五六七八九十'".as_bytes()
        ));
        assert!(mentions_name(true, "kikiki", "一二三四五六".as_bytes()));
        assert!(mentions_name(
            true,
            "orders",
            "ALTER TABLE shop.orders COMMENT='一'".as_bytes()
        ));
        let one_unit = "o\u{0130}".to_string() + "rders";
        assert!(!mentions_name(true, "orders", one_unit.as_bytes()));
        let spelled = "ship".replace('i', "\u{0130}");
        assert!(mentions_name(true, "ship", spelled.as_bytes()));
        assert!(mentions_name(
            true,
            "kinds",
            format!("{KELVIN}inds").as_bytes()
        ));
        assert!(!mentions_name(
            true,
            "linds",
            format!("{KELVIN}inds").as_bytes()
        ));
    }

    /// The set of folded characters is the `folds` of the statement-truth corpus header.
    #[test]
    fn the_folded_characters_are_the_ones_the_corpus_header_names() {
        let header: Vec<(u32, char)> = vec![(304, 'i'), (8490, 'k')];
        let ours: Vec<(u32, char)> = FOLDED_TO_ASCII
            .iter()
            .map(|&(c, l)| (c, char::from(l)))
            .collect();
        assert_eq!(ours, header);
        assert_eq!(char::from_u32(8490), Some('\u{212a}'));
        assert_eq!(char::from_u32(304), Some('\u{0130}'));
    }

    /// Every shape of four megabytes is read in one pass against ten names, inside the budget.
    #[test]
    fn a_hostile_four_megabytes_is_read_in_one_pass() {
        const MB4: usize = 4 << 20;
        let names: Vec<String> = [
            "kikikikikikikikiki",
            "ikikikikikikikikik",
            "kkkkkkkkkkkkkkkkkk",
            "iiiiiiiiiiiiiiiiii",
            "kikkikkikkikkikkik",
            "orders_0000000001",
            "orders_0000000002",
            "customers_kinds_i",
            "ikikikikikikikikix",
            "xkikikikikikikikik",
        ]
        .iter()
        .map(|n| n.to_string())
        .collect();
        let shapes: [(&str, Vec<u8>, usize); 4] = [
            ("non-ASCII", "中文".repeat(MB4 / 6).into_bytes(), 5),
            (
                "non-ASCII runs",
                "中文中文, ".repeat(MB4 / 14).into_bytes(),
                0,
            ),
            ("mixed", "ab中c".repeat(MB4 / 6).into_bytes(), 0),
            ("ASCII", b"a".repeat(MB4), 0),
        ];
        for (label, text, owed) in shapes {
            let names = names.clone();
            let (done, result) = std::sync::mpsc::channel();
            std::thread::spawn(move || {
                let hits: Vec<bool> = names
                    .iter()
                    .map(|n| mentions_name(true, n, &text))
                    .collect();
                let _ = done.send(hits);
            });
            let hits = result
                .recv_timeout(std::time::Duration::from_secs(10))
                .unwrap_or_else(|why| panic!("NAME SEARCH NOT LINEAR: {label}: {why}"));
            assert_eq!(
                hits.iter().filter(|h| **h).count(),
                owed,
                "{label}: {hits:?}"
            );
        }
    }

    /// The set-per-position reading of the rule, to compare the one-pass search with.
    fn folds_inside_by_sets(name: &str, text: &[u8]) -> bool {
        let ends = |at: usize| {
            let mut found: Vec<usize> = Vec::new();
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
                found.extend(&next);
                frontier = next;
            }
            found
        };
        let mut reach = vec![true; text.len() + 1];
        for c in name.bytes() {
            let mut next = vec![false; text.len() + 1];
            for at in (0..text.len()).filter(|&at| reach[at]) {
                if text[at] < 0x80 {
                    if text[at].eq_ignore_ascii_case(&c) {
                        next[at + 1] = true;
                    }
                } else if folded_from_non_ascii(c) {
                    for end in ends(at) {
                        next[end] = true;
                    }
                }
            }
            reach = next;
        }
        reach.iter().any(|r| *r)
    }

    /// The one-pass search agrees with the set-per-position reading on generated names and texts.
    #[test]
    fn the_one_pass_search_agrees_with_the_reading_by_sets() {
        let alphabet = [b'k', b'K', b'i', b'x', 0x81, 0x90, 0xaa, 0x32, 0x60, b'y'];
        let name_letters = [b'k', b'i', b'x', b'K'];
        let mut seed = 0x9e37_79b9_7f4a_7c15_u64;
        let mut roll = move |n: usize| {
            seed = seed
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            ((seed >> 33) as usize) % n
        };
        let mut seen = [0usize; 2];
        for _ in 0..40_000 {
            let name: String = (0..1 + roll(5))
                .map(|_| char::from(name_letters[roll(name_letters.len())]))
                .collect();
            let text: Vec<u8> = (0..roll(14))
                .map(|_| alphabet[roll(alphabet.len())])
                .collect();
            let want = folds_inside_by_sets(&name, &text);
            assert_eq!(folds_inside(&name, &text), want, "{name:?} in {text:?}");
            seen[usize::from(want)] += 1;
        }
        assert!(seen[0] > 2_000 && seen[1] > 2_000, "{seen:?}");
    }
}
