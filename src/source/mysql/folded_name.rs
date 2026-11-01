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

/// The byte spelling of identifiers in the client character set of a statement: what decides which byte sequences stand for the letters the server folds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Spelling {
    Utf8,
    Gb18030,
    Ujis,
    Latin5,
    Plain,
    Unknown,
}

const UTF8_SPELLINGS: &[(&[u8], u8)] = &[(&[0xC4, 0xB0], b'i'), (&[0xE2, 0x84, 0xAA], b'k')];
const GB18030_SPELLINGS: &[(&[u8], u8)] = &[
    (&[0x81, 0x30, 0x90, 0x32], b'i'),
    (&[0x81, 0x36, 0xBF, 0x32], b'k'),
];
const UJIS_SPELLINGS: &[(&[u8], u8)] = &[(&[0x8F, 0xAA, 0xC4], b'i')];
const LATIN5_SPELLINGS: &[(&[u8], u8)] = &[(&[0xDD], b'i')];
const EVERY_SPELLING: &[&[(&[u8], u8)]] = &[
    UTF8_SPELLINGS,
    GB18030_SPELLINGS,
    UJIS_SPELLINGS,
    LATIN5_SPELLINGS,
];

/// Collation ids of character sets that cannot spell a character the server folds to ASCII (every one is its own letter or an error), the ids of `information_schema.COLLATIONS` on 8.0.46 and 8.4.13.
const PLAIN_COLLATIONS: &[(u16, u16)] = &[
    (1, 11),
    (13, 16),
    (18, 29),
    (31, 32),
    (34, 44),
    (47, 62),
    (64, 75),
    (77, 77),
    (79, 82),
    (84, 90),
    (92, 96),
    (99, 99),
    (101, 124),
    (128, 151),
    (159, 183),
];

impl Spelling {
    /// The spelling of the client character set a collation id names: `Unknown` for an id nothing is known of, which tries every spelling.
    pub(crate) fn of_collation(id: Option<u16>) -> Self {
        match id {
            None => Self::Unknown,
            Some(12 | 97 | 98 | 91) => Self::Ujis,
            Some(30 | 78) => Self::Latin5,
            Some(248..=250) => Self::Gb18030,
            Some(33 | 45 | 46 | 63 | 76 | 83 | 192..=247 | 255..) => Self::Utf8,
            Some(id) if PLAIN_COLLATIONS.iter().any(|&(a, b)| (a..=b).contains(&id)) => Self::Plain,
            Some(_) => Self::Unknown,
        }
    }

    fn spellings(self) -> &'static [&'static [(&'static [u8], u8)]] {
        match self {
            Self::Utf8 => &[UTF8_SPELLINGS],
            Self::Gb18030 => &[GB18030_SPELLINGS],
            Self::Ujis => &[UJIS_SPELLINGS],
            Self::Latin5 => &[LATIN5_SPELLINGS],
            Self::Plain => &[],
            Self::Unknown => EVERY_SPELLING,
        }
    }

    fn is_utf8_text(self) -> bool {
        matches!(self, Self::Utf8 | Self::Plain | Self::Unknown)
    }
}

const BYTE_BASE: u32 = 0xF700;

/// Statement bytes as text that keeps every byte: UTF-8 text as it is, anything else with each byte from 0x80 up as one private-use character (`bytes_of` undoes it, and the flag says which of the two was made).
pub(crate) fn lossless_text(raw: &[u8], spelling: Spelling) -> (String, bool) {
    if spelling.is_utf8_text()
        && let Ok(text) = std::str::from_utf8(raw)
    {
        return (text.to_string(), false);
    }
    let text = raw
        .iter()
        .map(|&b| {
            if b < 0x80 {
                char::from(b)
            } else {
                char::from_u32(BYTE_BASE + u32::from(b)).unwrap_or(char::REPLACEMENT_CHARACTER)
            }
        })
        .collect();
    (text, true)
}

/// The bytes a text stands for: a private-use character made by [`lossless_text`] is its byte, any other character its UTF-8.
fn bytes_of(text: &str, byte_per_char: bool) -> Vec<u8> {
    if !byte_per_char {
        return text.as_bytes().to_vec();
    }
    let mut out = Vec::with_capacity(text.len());
    let mut buf = [0u8; 4];
    for c in text.chars() {
        match u32::from(c).checked_sub(BYTE_BASE) {
            Some(n @ 0x80..=0xFF) => out.push(n as u8),
            _ => out.extend_from_slice(c.encode_utf8(&mut buf).as_bytes()),
        }
    }
    out
}

/// The name as the server compares it on a server that folds: ASCII lower case, with each spelling of a folded letter in this client character set replaced by the letter, every other byte kept.
pub(crate) fn folded(name: &str, byte_per_char: bool, spelling: Spelling) -> String {
    let bytes = bytes_of(name, byte_per_char);
    let spellings = spelling.spellings();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let spelled = spellings
            .iter()
            .flat_map(|set| set.iter())
            .find(|(seq, _)| bytes[i..].starts_with(seq));
        match spelled {
            Some((seq, letter)) => {
                out.push(*letter);
                i += seq.len();
            }
            None => {
                out.push(bytes[i].to_ascii_lowercase());
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
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

    /// A name of 127 bytes is searched under the fold, one of 128 bytes only by ASCII case, and neither panics.
    #[test]
    fn the_longest_name_the_fold_search_takes_is_127_bytes() {
        let kelvin = KELVIN.as_bytes();
        for (len, folded) in [(127, true), (128, false)] {
            let name = format!("k{}", "a".repeat(len - 1));
            let text = [kelvin, "a".repeat(len - 1).as_bytes()].concat();
            assert_eq!(mentions_name(true, &name, &text), folded, "{len}");
            assert!(mentions_name(
                true,
                &name,
                [b"x".as_slice(), name.as_bytes(), "\u{4e00}".as_bytes()]
                    .concat()
                    .as_slice()
            ));
        }
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

    /// A shape of `bytes` bytes for the hostile-text test, with the number of the ten names it holds.
    fn hostile_shapes(bytes: usize) -> [(&'static str, Vec<u8>, usize); 4] {
        [
            ("non-ASCII", "中文".repeat(bytes / 6).into_bytes(), 5),
            (
                "non-ASCII runs",
                "中文中文, ".repeat(bytes / 14).into_bytes(),
                0,
            ),
            ("mixed", "ab中c".repeat(bytes / 6).into_bytes(), 0),
            ("ASCII", b"a".repeat(bytes), 0),
        ]
    }

    /// Time spent and names found by `mentions_name` over `text` for every name.
    fn timed_search(names: &[String], text: &[u8]) -> (std::time::Duration, usize) {
        let started = std::time::Instant::now();
        let hits = names
            .iter()
            .filter(|n| mentions_name(true, n, text))
            .count();
        (started.elapsed(), hits)
    }

    /// Four times the text costs under eight times the time, whatever else the machine is doing.
    #[test]
    fn a_hostile_four_megabytes_is_read_in_one_pass() {
        const MB: usize = 1 << 20;
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
        let small = hostile_shapes(MB);
        let large = hostile_shapes(4 * MB);
        for ((label, small, _), (_, large, owed)) in small.into_iter().zip(large) {
            let names = names.clone();
            let (done, result) = std::sync::mpsc::channel();
            std::thread::spawn(move || {
                let _ = done.send((timed_search(&names, &small), timed_search(&names, &large)));
            });
            let ((t_small, hits_small), (t_large, hits_large)) = result
                .recv_timeout(std::time::Duration::from_secs(120))
                .unwrap_or_else(|why| {
                    panic!("NAME SEARCH NOT LINEAR: {label}: no answer in 120 s: {why}")
                });
            assert_eq!((hits_small, hits_large), (owed, owed), "{label}");
            let floor = std::time::Duration::from_millis(50);
            let ratio = t_large.as_secs_f64() / t_small.max(floor).as_secs_f64();
            assert!(
                ratio < 8.0,
                "NAME SEARCH NOT LINEAR: {label}: 1 MB {t_small:?}, 4 MB {t_large:?}, x{ratio:.1}"
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

    fn spelled_in(spelling: Spelling, cp: u32) -> Vec<u8> {
        let letter = FOLDED_TO_ASCII.iter().find(|&&(c, _)| c == cp).unwrap().1;
        let set: &[(&[u8], u8)] = match spelling {
            Spelling::Utf8 => UTF8_SPELLINGS,
            Spelling::Gb18030 => GB18030_SPELLINGS,
            Spelling::Ujis => UJIS_SPELLINGS,
            Spelling::Latin5 => LATIN5_SPELLINGS,
            other => panic!("{other:?} has no table"),
        };
        set.iter()
            .find(|&&(_, l)| l == letter)
            .map(|&(seq, _)| seq.to_vec())
            .unwrap_or_default()
    }

    /// The utf8 spelling table is exactly the UTF-8 encoding of each code point the server folds to ASCII.
    #[test]
    fn the_utf8_spellings_are_the_encodings_of_the_folded_code_points() {
        for &(cp, letter) in &FOLDED_TO_ASCII {
            let ch = char::from_u32(cp).unwrap();
            let want = ch.to_string().into_bytes();
            assert_eq!(spelled_in(Spelling::Utf8, cp), want, "U+{cp:04X}");
            let name = format!("{ch}x");
            assert_eq!(
                folded(&name, false, Spelling::Utf8),
                format!("{}x", char::from(letter))
            );
        }
    }

    /// A multibyte character set spells a folded letter in its own bytes; the same bytes in another set are not that letter.
    #[test]
    fn each_character_set_folds_only_its_own_spelling() {
        let dotted_i = 0x0130;
        let kelvin = 0x212A;
        assert_eq!(folded("\u{f700}", false, Spelling::Utf8), "\u{f700}");
        let gb = lossless_text(&[0x81, 0x30, 0x90, 0x32, b'x'], Spelling::Gb18030);
        assert!(gb.1);
        assert_eq!(folded(&gb.0, gb.1, Spelling::Gb18030), "ix");
        assert_ne!(folded(&gb.0, gb.1, Spelling::Utf8), "ix");
        let uj = lossless_text(&[0x8F, 0xAA, 0xC4, b'x'], Spelling::Ujis);
        assert_eq!(folded(&uj.0, uj.1, Spelling::Ujis), "ix");
        assert_ne!(folded(&uj.0, uj.1, Spelling::Latin5), "ix");
        let l5 = lossless_text(&[0xDD, b'x'], Spelling::Latin5);
        assert_eq!(folded(&l5.0, l5.1, Spelling::Latin5), "ix");
        assert_ne!(folded(&l5.0, l5.1, Spelling::Plain), "ix");
        assert_eq!(
            spelled_in(Spelling::Gb18030, kelvin),
            vec![0x81, 0x36, 0xBF, 0x32]
        );
        assert!(spelled_in(Spelling::Ujis, kelvin).is_empty());
        assert!(spelled_in(Spelling::Latin5, kelvin).is_empty());
        assert_eq!(spelled_in(Spelling::Latin5, dotted_i), vec![0xDD]);
    }

    /// An unknown collation tries every spelling; a plain one tries none.
    #[test]
    fn an_unknown_collation_tries_every_spelling_and_a_plain_one_none() {
        let utf = "\u{130}x";
        assert_eq!(folded(utf, false, Spelling::Unknown), "ix");
        assert_eq!(folded(utf, false, Spelling::Plain), "\u{130}x");
        assert_eq!(Spelling::of_collation(None), Spelling::Unknown);
        assert_eq!(Spelling::of_collation(Some(251)), Spelling::Unknown);
        assert_eq!(Spelling::of_collation(Some(8)), Spelling::Plain);
        assert_eq!(Spelling::of_collation(Some(255)), Spelling::Utf8);
        assert_eq!(Spelling::of_collation(Some(33)), Spelling::Utf8);
        assert_eq!(Spelling::of_collation(Some(63)), Spelling::Utf8);
        assert_eq!(Spelling::of_collation(Some(248)), Spelling::Gb18030);
        assert_eq!(Spelling::of_collation(Some(12)), Spelling::Ujis);
        assert_eq!(Spelling::of_collation(Some(30)), Spelling::Latin5);
    }

    /// Every byte string survives the lossless view: lossless_text then bytes_of gives the same bytes.
    #[test]
    fn the_lossless_text_round_trips_every_byte() {
        let all: Vec<u8> = (0..=255u8).collect();
        for spelling in [
            Spelling::Utf8,
            Spelling::Plain,
            Spelling::Gb18030,
            Spelling::Unknown,
        ] {
            let (text, per_byte) = lossless_text(&all, spelling);
            assert!(
                per_byte,
                "{spelling:?}: a non-UTF-8 string takes the byte-per-character view"
            );
            assert_eq!(bytes_of(&text, per_byte), all);
        }
        let valid = "TRUNCATE `\u{130}x` \u{65e5}".as_bytes();
        let (text, per_byte) = lossless_text(valid, Spelling::Utf8);
        assert!(!per_byte);
        assert_eq!(bytes_of(&text, per_byte), valid);
        let (text, per_byte) = lossless_text(valid, Spelling::Gb18030);
        assert!(per_byte);
        assert_eq!(bytes_of(&text, per_byte), valid);
    }

    /// A megabyte of hostile bytes is read in linear time and never panics.
    #[test]
    fn a_megabyte_of_hostile_bytes_is_folded_in_one_pass() {
        let mut hostile = Vec::with_capacity(1 << 20);
        while hostile.len() < 1 << 20 {
            hostile.extend_from_slice(&[
                0xC4, 0xB0, 0xE2, 0x84, 0xAA, 0x81, 0x30, 0x90, 0x32, 0xFF, 0x00, b'K',
            ]);
        }
        let start = std::time::Instant::now();
        for spelling in [Spelling::Utf8, Spelling::Gb18030, Spelling::Unknown] {
            let (text, per_byte) = lossless_text(&hostile, spelling);
            let out = folded(&text, per_byte, spelling);
            assert!(!out.is_empty());
        }
        assert!(
            start.elapsed() < std::time::Duration::from_secs(5),
            "{:?}",
            start.elapsed()
        );
    }
}
