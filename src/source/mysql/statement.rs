//! One reading of a MySQL statement as the server lexes it: quotes, comments and executable comments.

use super::client_charsets::COLLATION_CHARSET;

/// One lexical piece of a statement; the raw text of the pieces in order is the statement with its comments read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Tok<'a> {
    Blank(&'a str),
    Word(&'a str),
    Quoted {
        quote: char,
        raw: &'a str,
        closed: bool,
    },
    Punct(char),
}

impl<'a> Tok<'a> {
    /// The text the piece renders as.
    #[cfg(test)]
    fn raw(&self, out: &mut String) {
        match self {
            Tok::Blank(s) | Tok::Word(s) => out.push_str(s),
            Tok::Quoted { raw, .. } => out.push_str(raw),
            Tok::Punct(c) => out.push(*c),
        }
    }

    /// The name a quoted identifier holds (backtick, or double quote under `ANSI_QUOTES`), with its doubled quotes undone.
    pub(crate) fn identifier(&self) -> Option<String> {
        match self {
            Tok::Quoted {
                quote: quote @ ('`' | '"'),
                raw,
                closed: true,
            } => {
                let inner = &raw[1..raw.len() - 1];
                Some(inner.replace(&format!("{quote}{quote}"), &quote.to_string()))
            }
            _ => None,
        }
    }
}

/// The `sql_mode` flags that change how a statement is lexed; the default is the lexing used when an event carries no `sql_mode`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct SqlMode {
    pub(crate) no_backslash_escapes: bool,
    pub(crate) ansi_quotes: bool,
}

impl SqlMode {
    const ANSI_QUOTES: u64 = 0x4;
    const NO_BACKSLASH_ESCAPES: u64 = 0x10_0000;

    /// The mode a binlog `sql_mode` status variable holds.
    pub(crate) fn from_bits(bits: u64) -> Self {
        Self {
            no_backslash_escapes: bits & Self::NO_BACKSLASH_ESCAPES != 0,
            ansi_quotes: bits & Self::ANSI_QUOTES != 0,
        }
    }

    /// Whether a backslash escapes the next character inside `quote`.
    fn escapes_in(self, quote: char) -> bool {
        match quote {
            '\'' => !self.no_backslash_escapes,
            '"' => !self.no_backslash_escapes && !self.ansi_quotes,
            _ => false,
        }
    }
}

/// How many version digits a `/*!` marker consumes: none under five digits, six when the sixth digit is followed by whitespace, else five.
fn version_digits(after_bang: &[u8]) -> usize {
    match after_bang {
        [
            b'0'..=b'9',
            b'0'..=b'9',
            b'0'..=b'9',
            b'0'..=b'9',
            b'0'..=b'9',
            b'0'..=b'9',
            b' ' | 9..=13,
            ..,
        ] => 6,
        [
            b'0'..=b'9',
            b'0'..=b'9',
            b'0'..=b'9',
            b'0'..=b'9',
            b'0'..=b'9',
            ..,
        ] => 5,
        _ => 0,
    }
}

fn word_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_' || c == '$' || !c.is_ascii() && !c.is_whitespace()
}

fn char_at(sql: &str, i: usize) -> Option<char> {
    sql.get(i..).and_then(|s| s.chars().next())
}

/// Where the quote opened at `start` ends (one past its closer, or the end of the text) and whether it was closed.
fn quote_end(sql: &str, start: usize, quote: char, mode: SqlMode) -> (usize, bool) {
    let (b, q, escapes) = (sql.as_bytes(), quote as u8, mode.escapes_in(quote));
    let mut j = start + 1;
    while j < b.len() {
        match b[j] {
            b'\\' if escapes => j += 2,
            c if c == q && b.get(j + 1) == Some(&q) => j += 2,
            c if c == q => return (j + 1, true),
            _ => j += char_at(sql, j).map_or(1, char::len_utf8),
        }
    }
    (sql.len(), false)
}

/// Whether `rest` opens a line comment: `#`, or `--` followed by whitespace, a control character or the end.
fn line_comment_starts(rest: &str) -> bool {
    rest.starts_with('#')
        || rest.strip_prefix("--").is_some_and(|after| {
            after
                .bytes()
                .next()
                .is_none_or(|n| n.is_ascii_whitespace() || n.is_ascii_control())
        })
}

/// A client character set whose multibyte characters can end in a backslash or a backtick byte.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Layout {
    Big5,
    Gbk,
    Sjis,
    Gb18030,
}

/// The multibyte client character sets and their layouts.
const LAYOUTS: &[(&str, Layout)] = &[
    ("big5", Layout::Big5),
    ("gbk", Layout::Gbk),
    ("sjis", Layout::Sjis),
    ("cp932", Layout::Sjis),
    ("gb18030", Layout::Gb18030),
];

impl Layout {
    fn of_name(name: &str) -> Option<Self> {
        LAYOUTS.iter().find(|(n, _)| *n == name).map(|(_, l)| *l)
    }

    fn encoding(self) -> &'static encoding_rs::Encoding {
        match self {
            Self::Big5 => encoding_rs::BIG5,
            Self::Gbk => encoding_rs::GBK,
            Self::Sjis => encoding_rs::SHIFT_JIS,
            Self::Gb18030 => encoding_rs::GB18030,
        }
    }

    /// The byte length of the character at the start of `b`, as the server's `my_ismbchar` counts it.
    fn char_len(self, b: &[u8]) -> usize {
        let wide = matches!(
            (self, b),
            (
                Self::Gb18030,
                [0x81..=0xFE, 0x30..=0x39, 0x81..=0xFE, 0x30..=0x39, ..]
            )
        );
        let pair = matches!(
            (self, b),
            (Self::Big5, [0xA1..=0xF9, 0x40..=0x7E | 0xA1..=0xFE, ..])
                | (Self::Sjis, [0x81..=0x9F | 0xE0..=0xFC, 0x40..=0x7E | 0x80..=0xFC, ..])
                | (Self::Gbk | Self::Gb18030, [0x81..=0xFE, 0x40..=0x7E | 0x80..=0xFE, ..])
        );
        if wide {
            4
        } else if pair {
            2
        } else {
            1
        }
    }
}

/// The character a byte the reader cannot place is read as: a letter to the lexer, and every consumer also reads the statement with it a blank.
pub(crate) const UNPLACED: char = '\u{FFFD}';

/// How the server cuts a statement into characters in a client character set.
enum Cut {
    Utf8,
    Bytes,
    Pairs(Layout),
}

impl Cut {
    /// The cut of the character set a QUERY event's client used (its collation id); the UTF-8 family, `binary` and an unrecorded set are read as UTF-8.
    fn of_collation(id: Option<u16>) -> Self {
        match id.map(charset_name) {
            None | Some(Some("utf8mb3" | "utf8mb4" | "binary" | "ascii")) => Self::Utf8,
            Some(Some(n)) => Layout::of_name(n).map_or(Self::Bytes, Self::Pairs),
            Some(None) => Self::Bytes,
        }
    }
}

/// The name of the character set a collation id belongs to, if this reader knows the id.
pub(crate) fn charset_name(id: u16) -> Option<&'static str> {
    COLLATION_CHARSET
        .binary_search_by_key(&id, |(i, _)| *i)
        .ok()
        .map(|at| COLLATION_CHARSET[at].1)
}

/// The one character `text` decodes to when it is a non-blank one (a pair never decodes to ASCII); else the marker of a byte the reader cannot place.
fn plain(text: &str) -> char {
    let mut chars = text.chars();
    match (chars.next(), chars.next()) {
        (Some(c), None) if c != UNPLACED && !c.is_whitespace() => c,
        _ => UNPLACED,
    }
}

/// A control byte other than ASCII whitespace, which a server may read as a blank or as a letter.
fn unplaced_control(c: char) -> bool {
    matches!(c, '\0'..='\x08' | '\x0E'..='\x1F')
}

/// `raw` cut into characters: a pair of `layout` is the character it encodes, every other byte of 0x80 and above the marker.
fn cut(raw: &[u8], layout: Option<Layout>) -> String {
    let mut out = String::with_capacity(raw.len());
    let mut rest = raw;
    while let Some(&first) = rest.first() {
        let width = layout.map_or(1, |l| l.char_len(rest).max(1));
        let (one, tail) = rest.split_at(width);
        out.push(match (layout, width) {
            (Some(l), 2..) => plain(&l.encoding().decode_without_bom_handling(one).0),
            _ => {
                if first.is_ascii() {
                    char::from(first)
                } else {
                    UNPLACED
                }
            }
        });
        rest = tail;
    }
    out
}

/// A statement's bytes as text, cut into characters where the server cuts them in the client character set (its collation id); a byte the reader cannot place, and a control byte, is the marker.
pub(crate) fn client_text(raw: &[u8], charset: Option<u16>) -> std::borrow::Cow<'_, str> {
    let text = match Cut::of_collation(charset) {
        Cut::Utf8 => String::from_utf8_lossy(raw),
        Cut::Bytes => cut(raw, None).into(),
        Cut::Pairs(layout) => cut(raw, Some(layout)).into(),
    };
    text.replace(unplaced_control, "\u{FFFD}").into()
}

/// The statement cut into pieces the way the server lexes it: `/*! */` bodies are SQL (nested ones too), other comments, `#` and `-- ` lines become a blank, quoted text is one piece.
pub(crate) fn tokens(sql: &str, mode: SqlMode) -> Vec<Tok<'_>> {
    let b = sql.as_bytes();
    let (mut out, mut i, mut executable) = (Vec::new(), 0usize, 0usize);
    while let Some(c) = char_at(sql, i) {
        let rest = &sql[i..];
        let line_end = |from: usize| sql[from..].find('\n').map_or(sql.len(), |n| from + n);
        if c.is_whitespace() {
            let n = rest
                .find(|ch: char| !ch.is_whitespace())
                .unwrap_or(rest.len());
            out.push(Tok::Blank(&rest[..n]));
            i += n;
        } else if rest.starts_with("/*!") {
            executable += 1;
            out.push(Tok::Blank(" "));
            let digits = version_digits(&b[i + 3..]);
            i = sql
                .len()
                .saturating_sub(rest.get(3 + digits..).map_or(0, str::len));
        } else if let Some(body) = rest.strip_prefix("/*") {
            out.push(Tok::Blank(" "));
            i = body.find("*/").map_or(sql.len(), |n| i + n + 4);
        } else if executable > 0 && rest.starts_with("*/") {
            executable -= 1;
            out.push(Tok::Blank(" "));
            i += 2;
        } else if line_comment_starts(rest) {
            out.push(Tok::Blank(" "));
            i = line_end(i);
        } else if matches!(c, '\'' | '"' | '`') {
            let (end, closed) = quote_end(sql, i, c, mode);
            out.push(Tok::Quoted {
                quote: c,
                raw: &sql[i..end],
                closed,
            });
            i = end;
        } else if word_char(c) {
            let n = rest.find(|ch: char| !word_char(ch)).unwrap_or(rest.len());
            out.push(Tok::Word(&rest[..n]));
            i += n;
        } else {
            out.push(Tok::Punct(c));
            i += c.len_utf8();
        }
    }
    out
}

/// A statement text as the server executes it: `/*! */` bodies are SQL, other comments become a space, quoted text is kept as written.
#[cfg(test)]
pub(crate) fn executable_text(sql: &str, mode: SqlMode) -> String {
    let mut out = String::with_capacity(sql.len());
    tokens(sql, mode).iter().for_each(|t| t.raw(&mut out));
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::mysql::client_charsets::PAIR_LEADS;

    const NO_BACKSLASH: SqlMode = SqlMode {
        no_backslash_escapes: true,
        ansi_quotes: false,
    };
    const ANSI: SqlMode = SqlMode {
        no_backslash_escapes: false,
        ansi_quotes: true,
    };

    fn read_in(sql: &str, mode: SqlMode) -> String {
        executable_text(sql, mode)
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
    }

    fn read(sql: &str) -> String {
        read_in(sql, SqlMode::default())
    }

    #[test]
    fn every_comment_kind_is_a_blank_and_quoted_text_is_kept_as_written() {
        for (sql, want) in [
            ("a -- c\nb", "a b"),
            ("a --\tc\nb", "a b"),
            ("a --", "a"),
            ("a --c\nb", "a --c b"),
            ("a # c\nb", "a b"),
            ("a # c", "a"),
            ("a /* c */ b", "a b"),
            ("a /**/ b", "a b"),
            ("a /*/ b", "a"),
            ("'a -- b' c", "'a -- b' c"),
            ("'a # b' c", "'a # b' c"),
            ("'a /* b */' c", "'a /* b */' c"),
            ("\"a -- b\" c", "\"a -- b\" c"),
            ("`a -- b` c", "`a -- b` c"),
            ("'it''s -- x' -- y\nz", "'it''s -- x' z"),
            ("'it\\'s -- x' -- y\nz", "'it\\'s -- x' z"),
            ("'\u{e9}\\'x -- y' -- z\nw", "'\u{e9}\\'x -- y' w"),
            ("'\\\u{e9}' -- z\nw", "'\\\u{e9}' w"),
            ("'\\'' -- c\nd", "'\\'' d"),
            ("`a``b -- c` d", "`a``b -- c` d"),
            ("`a\\` -- c\nd", "`a\\` d"),
            ("\"a\"\"b # c\" d", "\"a\"\"b # c\" d"),
            ("/*!80000 -- c\nREVOKE */", "REVOKE"),
            ("/*!80000 # c\nREVOKE */", "REVOKE"),
            ("/*!80000 /* c */ REVOKE */", "REVOKE"),
            ("/*!80000 x /*!50000 y */ z */", "x y z"),
            ("/*!80000 a */ b */ c", "a b */ c"),
            ("/*!80000 'a */ b' */", "'a */ b'"),
            (
                "/*!80000 \u{540d} -- \u{540d}\n\u{540d} */",
                "\u{540d} \u{540d}",
            ),
            ("-- \u{540d}\n\u{540d}", "\u{540d}"),
            ("\u{540d} # \u{540d}", "\u{540d}"),
        ] {
            assert_eq!(read(sql), want, "{sql:?}");
        }
    }

    #[test]
    fn a_quoted_name_is_an_identifier_only_when_closed_and_has_its_doubled_quotes_undone() {
        let name = |sql: &str| {
            tokens(sql, SqlMode::default())
                .into_iter()
                .find_map(|t| t.identifier())
        };
        for (sql, want) in [
            ("`a`", Some("a")),
            ("`a``b`", Some("a`b")),
            ("\"a\"", Some("a")),
            ("\"a\"\"b\"", Some("a\"b")),
            ("\"\u{540d}\"", Some("\u{540d}")),
            ("'a'", None),
            ("`a", None),
            ("\"a", None),
            ("a", None),
        ] {
            assert_eq!(name(sql).as_deref(), want, "{sql:?}");
        }
    }

    #[test]
    fn an_unterminated_quote_swallows_the_rest_and_a_comment_marker_inside_it_is_text() {
        let toks = tokens("x 'a -- b\nc", SqlMode::default());
        assert!(matches!(
            toks.last(),
            Some(Tok::Quoted {
                closed: false,
                quote: '\'',
                ..
            })
        ));
        assert_eq!(
            executable_text("x 'a -- b\nc", SqlMode::default()),
            "x 'a -- b\nc"
        );
    }

    #[test]
    fn a_binlog_sql_mode_with_no_flag_reads_as_the_default_and_each_flag_is_read_on_its_own() {
        assert_eq!(SqlMode::from_bits(0), SqlMode::default());
        assert_eq!(SqlMode::from_bits(0x4), ANSI);
        assert_eq!(SqlMode::from_bits(0x10_0000), NO_BACKSLASH);
        assert_eq!(
            SqlMode::from_bits(0x4 | 0x10_0000 | 0x1),
            SqlMode {
                no_backslash_escapes: true,
                ansi_quotes: true
            }
        );
        assert_eq!(SqlMode::from_bits(!(0x4 | 0x10_0000)), SqlMode::default());
    }

    #[test]
    fn a_backslash_escapes_in_a_quote_only_when_the_mode_lets_it() {
        for (sql, mode, want) in [
            ("'a\\' b' -- c\nd", SqlMode::default(), "'a\\' b' d"),
            ("'a\\' b' -- c\nd", ANSI, "'a\\' b' d"),
            ("'a\\' b' -- c\nd", NO_BACKSLASH, "'a\\' b' -- c d"),
            ("\"a\\\" b\" -- c\nd", SqlMode::default(), "\"a\\\" b\" d"),
            ("\"a\\\" b\" -- c\nd", NO_BACKSLASH, "\"a\\\" b\" -- c d"),
            ("\"a\\\" b\" -- c\nd", ANSI, "\"a\\\" b\" -- c d"),
            ("`a\\` -- c\nd", SqlMode::default(), "`a\\` d"),
            ("`a\\` -- c\nd", ANSI, "`a\\` d"),
            ("`a\\` -- c\nd", NO_BACKSLASH, "`a\\` d"),
            ("\"a\"\"b\" -- c\nd", ANSI, "\"a\"\"b\" d"),
            ("'a''b' -- c\nd", NO_BACKSLASH, "'a''b' d"),
        ] {
            assert_eq!(read_in(sql, mode), want, "{sql:?} {mode:?}");
        }
    }

    #[test]
    fn a_double_quoted_piece_is_an_identifier_under_ansi_quotes_with_no_escape_inside() {
        let toks = tokens("\"x\\\" INT, y", ANSI);
        assert_eq!(toks[0].identifier().as_deref(), Some("x\\"));
        let toks = tokens("\"x\\\" INT, y", SqlMode::default());
        assert_eq!(toks[0].identifier(), None);
    }

    #[test]
    fn a_version_comment_takes_five_digits_or_six_when_whitespace_follows_the_sixth() {
        for (sql, want) in [
            ("/*!80000 REVOKE */", "REVOKE"),
            ("/*!080400 REVOKE */", "REVOKE"),
            ("/*!080400\tREVOKE */", "REVOKE"),
            ("/*!080400\nREVOKE */", "REVOKE"),
            ("/*!080400REVOKE */", "0REVOKE"),
            ("/*!080400", "0"),
            ("/*!0804000 x */", "00 x"),
            ("/*!800001 x */", "x"),
            ("/*!8000012345 x */", "12345 x"),
            ("/*!80000123 x */", "123 x"),
            ("/*!8000 x */", "8000 x"),
            ("/*!x */", "x"),
            ("/*! x */", "x"),
            ("/*!5000a x */", "5000a x"),
            ("/*!80000REVOKE */", "REVOKE"),
        ] {
            assert_eq!(read(sql), want, "{sql:?}");
        }
    }

    fn first_quote_is_closed(raw: &[u8], charset: u16) -> Option<bool> {
        let sql = client_text(raw, Some(charset));
        tokens(&sql, SqlMode::default())
            .iter()
            .find_map(|t| match t {
                Tok::Quoted { closed, .. } => Some(*closed),
                _ => None,
            })
    }

    #[test]
    fn a_multibyte_character_ending_in_a_backslash_does_not_escape_the_quote_after_it() {
        for (raw, charset) in [
            (&b"'\xE4\xB8\xAD\\'"[..], 28),
            (&b"'\xE4\xB8\xAD\\'"[..], 87),
            (&b"'\xC3\x81\\'"[..], 13),
            (&b"'\xC3\x81\\'"[..], 88),
            (&b"'\xC3\x81\\'"[..], 95),
            (&b"'\xC3\x81\\'"[..], 96),
            (&b"'\xA4\\'"[..], 1),
            (&b"'\xA4\\'"[..], 84),
            (&b"'\x81\x30\x81\x30\xAD\\'"[..], 248),
            (&b"'\x81\\'"[..], 249),
            (&b"'\x81\\'"[..], 250),
        ] {
            assert_eq!(
                first_quote_is_closed(raw, charset),
                Some(true),
                "{raw:?} {charset}"
            );
        }
    }

    #[test]
    fn the_same_bytes_in_a_character_set_without_such_characters_keep_the_backslash_an_escape() {
        for (raw, charset) in [
            (&b"'\xE4\xB8\xAD\\'"[..], 45),
            (&b"'\xE4\xB8\xAD\\'"[..], 255),
            (&b"'\xC3\x81\\'"[..], 8),
            (&b"'\xC3\x81\\'"[..], 63),
            (&b"'\xA4\\'"[..], 12),
            (&b"'\xA4\\'"[..], 19),
            (&b"'x\\'"[..], 28),
            (&b"'x\\'"[..], 13),
            (&b"'x\\'"[..], 1),
            (&b"'x\\'"[..], 248),
        ] {
            assert_eq!(
                first_quote_is_closed(raw, charset),
                Some(false),
                "{raw:?} {charset}"
            );
        }
    }

    #[test]
    fn a_four_byte_gb18030_character_and_a_lead_without_a_trail_are_read_as_the_server_counts_them()
    {
        assert_eq!(client_text(b"\x81\x30\x81\x30", Some(248)), "\u{80}");
        assert_eq!(
            first_quote_is_closed(b"'\x81\x30\x81\x30\\'", 248),
            Some(false)
        );
        assert_eq!(first_quote_is_closed(b"'\x81\x30\x81\\'", 248), Some(true));
        assert_eq!(client_text(b"A\x81", Some(28)), "A\u{FFFD}");
        assert_eq!(client_text(b"A\x81 B", Some(28)), "A\u{FFFD} B");
        assert_eq!(client_text(b"A\x81\x7F", Some(28)), "A\u{FFFD}\x7F");
        assert_eq!(client_text(b"A\xA4\x20", Some(1)), "A\u{FFFD} ");
        assert_eq!(client_text(b"A\xA0\x5C", Some(1)), "A\u{FFFD}\\");
    }

    #[test]
    fn a_multibyte_trail_that_is_a_backtick_does_not_close_the_identifier() {
        let open = |raw: &[u8], charset: u16| {
            let sql = client_text(raw, Some(charset));
            tokens(&sql, SqlMode::default())
                .iter()
                .find_map(|t| match t {
                    Tok::Quoted {
                        quote: '`', closed, ..
                    } => Some(*closed),
                    _ => None,
                })
        };
        assert_eq!(open(b"`a\x81`", 28), Some(false));
        assert_eq!(open(b"`a\x81`", 45), Some(true));
    }

    #[test]
    fn a_name_in_a_multibyte_character_set_is_decoded_to_the_text_the_server_stores() {
        assert_eq!(client_text(b"\xB1\xED", Some(28)), "\u{8868}");
        assert_eq!(client_text(b"\x95\x5C", Some(13)), "\u{8868}");
        assert_eq!(client_text(b"\x95\x5C", Some(95)), "\u{8868}");
        assert_eq!(
            client_text("t\u{8868}".as_bytes(), Some(255)),
            "t\u{8868}",
            "a UTF-8 client is read as before"
        );
        assert_eq!(client_text(b"select 1", None), "select 1");
    }

    fn default_id(name: &str) -> u16 {
        COLLATION_CHARSET
            .iter()
            .find(|(_, n)| *n == name)
            .map(|(id, _)| *id)
            .expect(name)
    }

    #[test]
    fn a_pair_the_encoding_standard_cannot_map_is_one_character_and_hides_its_trail() {
        for id in [13, 88, 95, 96] {
            assert_eq!(
                client_text(b"\xC5\x85\x5C", Some(id)),
                "\u{FFFD}\u{FFFD}",
                "{id}"
            );
            assert_eq!(
                first_quote_is_closed(b"'\xC5\x85\x5C'", id),
                Some(true),
                "{id}"
            );
        }
        assert_eq!(client_text(b"\x85\x5C", Some(88)), "\u{FFFD}");
    }

    #[test]
    fn every_lead_and_ascii_trail_is_cut_where_the_server_cuts_it() {
        for (name, leads) in PAIR_LEADS {
            let id = default_id(name);
            for lead in 0x80..=0xFFu8 {
                for trail in 0x20..=0x7Eu8 {
                    let raw = [lead, trail];
                    let text = client_text(&raw, Some(id));
                    let swallowed = leads.contains(&lead) && (0x40..=0x7E).contains(&trail);
                    let chars: Vec<char> = text.chars().collect();
                    if swallowed {
                        assert_eq!(chars.len(), 1, "{name} {lead:#x} {trail:#x}: {text:?}");
                        assert!(!chars[0].is_ascii(), "{name} {lead:#x} {trail:#x}");
                    } else {
                        assert_eq!(chars.len(), 2, "{name} {lead:#x} {trail:#x}: {text:?}");
                        assert_eq!(chars[1], char::from(trail), "{name} {lead:#x} {trail:#x}");
                        assert!(!chars[0].is_ascii(), "{name} {lead:#x} {trail:#x}");
                    }
                }
            }
        }
    }

    #[test]
    fn no_pair_of_a_multibyte_character_set_decodes_to_ascii_or_a_blank_the_lexer_would_read() {
        for (name, _) in PAIR_LEADS {
            let id = default_id(name);
            for lead in 0x81..=0xFEu8 {
                for trail in 0x30..=0xFEu8 {
                    let raw = [lead, trail];
                    let text = client_text(&raw, Some(id));
                    let mut chars = text.chars();
                    if let (Some(c), None) = (chars.next(), chars.next()) {
                        assert!(!c.is_ascii(), "{name} {lead:#x} {trail:#x}");
                    }
                }
            }
        }
        let gb = default_id("gb18030");
        for lead in 0x81..=0xFEu8 {
            for third in 0x81..=0xFEu8 {
                let raw = [lead, 0x30, third, 0x30];
                let text = client_text(&raw, Some(gb));
                assert!(text.chars().all(|c| !c.is_ascii()), "{lead:#x} {third:#x}");
            }
        }
    }

    #[test]
    fn a_pair_that_decodes_to_a_blank_is_a_marker_not_a_blank() {
        let sjis = default_id("sjis");
        assert_eq!(client_text(&[0x81, 0x40], Some(sjis)).as_ref(), "\u{FFFD}");
        assert_eq!(client_text(&[0x82, 0xA0], Some(sjis)).as_ref(), "\u{3042}");
    }

    #[test]
    fn every_byte_the_reader_cannot_place_is_one_marker_whatever_the_client_character_set() {
        let unclientable = ["ucs2", "utf16", "utf16le", "utf32"];
        for (id, name) in COLLATION_CHARSET {
            if unclientable.contains(name) {
                continue;
            }
            for byte in 0..=0xFFu8 {
                let bytes = [byte];
                let text = client_text(&bytes, Some(*id));
                let want = if byte.is_ascii() && !matches!(byte, 0..=8 | 0x0E..=0x1F) {
                    char::from(byte)
                } else {
                    UNPLACED
                };
                assert_eq!(text, want.to_string(), "{name} {byte:#x}");
            }
        }
        assert_eq!(
            client_text(b"\x01 a\tb\r\n\x0B\x0C", None),
            "\u{FFFD} a\tb\r\n\x0B\x0C"
        );
        assert_eq!(client_text(b"a\x1Fb", Some(255)), "a\u{FFFD}b");
        assert_eq!(client_text(b"a\x7Fb", Some(255)), "a\x7Fb");
        assert_eq!(client_text(b"caf\xE9", Some(8)), "caf\u{FFFD}");
        assert_eq!(client_text(b"caf\xC3\xA9", Some(255)), "caf\u{E9}");
    }

    #[test]
    fn a_character_set_this_reader_does_not_know_reads_every_high_byte_as_a_non_blank() {
        assert_eq!(client_text(b"A\xE9 B", Some(60000)), "A\u{FFFD} B");
        assert_eq!(client_text(b"A\xA0B", Some(60000)), "A\u{FFFD}B");
        assert_eq!(charset_name(8), Some("latin1"));
        assert_eq!(charset_name(60000), None);
        assert_eq!(charset_name(255), Some("utf8mb4"));
        assert_eq!(client_text("t\u{8868}".as_bytes(), Some(45)), "t\u{8868}");
        assert_eq!(client_text(b"A\xE9", Some(63)), "A\u{FFFD}");
    }

    #[test]
    fn a_version_marker_with_too_few_digits_takes_none() {
        for (after, want) in [
            (&b""[..], 0),
            (&b"8000"[..], 0),
            (&b"80000"[..], 5),
            (&b"800001"[..], 5),
            (&b"080400"[..], 5),
            (&b"080400 "[..], 6),
            (&b"080400\t"[..], 6),
            (&b"0804001 "[..], 5),
            (&b"8000 x"[..], 0),
        ] {
            assert_eq!(version_digits(after), want, "{after:?}");
        }
    }

    #[test]
    fn a_trail_byte_outside_the_wide_range_does_not_join_its_lead() {
        for t in [0x00u8, 0x20, 0x30, 0x3F, 0x7F, 0xFF] {
            assert_eq!(Layout::Gbk.char_len(&[0x81, t]), 1, "gbk {t:#x}");
            assert_eq!(
                Layout::Gb18030.char_len(&[0x81, t, 0x20, 0x20]),
                1,
                "gb18030 {t:#x}"
            );
        }
        for t in [0x40u8, 0x5C, 0x7E, 0x80, 0xFE] {
            assert_eq!(Layout::Gbk.char_len(&[0x81, t]), 2, "gbk {t:#x}");
            assert_eq!(
                Layout::Gb18030.char_len(&[0x81, t, 0x20, 0x20]),
                2,
                "gb18030 {t:#x}"
            );
        }
        assert_eq!(Layout::Gb18030.char_len(&[0x81, 0x30, 0x81, 0x30]), 4);
        assert_eq!(Layout::Gb18030.char_len(&[0x81, 0x30, 0x81, 0x20]), 1);
    }

    #[test]
    fn every_executable_comment_marker_is_walked_over_wherever_it_stands() {
        let sql = "x /*!80000 a */ /*!080400 b */ c";
        let text: String = tokens(sql, SqlMode::default())
            .iter()
            .map(|t| match t {
                Tok::Blank(_) => " ".to_string(),
                Tok::Word(w) => (*w).to_string(),
                _ => "?".to_string(),
            })
            .collect();
        assert_eq!(
            text.split_whitespace().collect::<Vec<_>>(),
            ["x", "a", "b", "c"]
        );
    }
}
