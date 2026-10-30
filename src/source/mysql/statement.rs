//! One reading of a MySQL statement as the server lexes it: quotes, comments and executable comments.

use super::client_charsets::{COLLATION_CHARSET, HIGH_BYTES, LEXICAL, PAIR_FIXES};

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

/// A multibyte client character set and the layout of its two-byte characters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Layout {
    Big5,
    Gbk,
    Sjis,
    Cp932,
    Gb18030,
    Gb2312,
    Ujis,
    EucJpMs,
    EucKr,
}

/// The multibyte client character sets and their layouts.
const LAYOUTS: &[(&str, Layout)] = &[
    ("big5", Layout::Big5),
    ("gbk", Layout::Gbk),
    ("sjis", Layout::Sjis),
    ("cp932", Layout::Cp932),
    ("gb18030", Layout::Gb18030),
    ("gb2312", Layout::Gb2312),
    ("ujis", Layout::Ujis),
    ("eucjpms", Layout::EucJpMs),
    ("euckr", Layout::EucKr),
];

impl Layout {
    fn of_name(name: &str) -> Option<Self> {
        LAYOUTS.iter().find(|(n, _)| *n == name).map(|(_, l)| *l)
    }

    fn name(self) -> &'static str {
        LAYOUTS
            .iter()
            .find(|(_, l)| *l == self)
            .map_or("", |(n, _)| n)
    }

    fn encoding(self) -> &'static encoding_rs::Encoding {
        match self {
            Self::Big5 => encoding_rs::BIG5,
            Self::Gbk | Self::Gb2312 => encoding_rs::GBK,
            Self::Sjis | Self::Cp932 => encoding_rs::SHIFT_JIS,
            Self::Gb18030 => encoding_rs::GB18030,
            Self::Ujis | Self::EucJpMs => encoding_rs::EUC_JP,
            Self::EucKr => encoding_rs::EUC_KR,
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
        let triple = matches!(
            (self, b),
            (
                Self::Ujis | Self::EucJpMs,
                [0x8F, 0xA1..=0xFE, 0xA1..=0xFE, ..]
            )
        );
        let pair = matches!(
            (self, b),
            (Self::Big5, [0xA1..=0xF9, 0x40..=0x7E | 0xA1..=0xFE, ..])
                | (
                    Self::Sjis | Self::Cp932,
                    [0x81..=0x9F | 0xE0..=0xFC, 0x40..=0x7E | 0x80..=0xFC, ..]
                )
                | (Self::Gbk | Self::Gb18030, [0x81..=0xFE, 0x40..=0x7E | 0x80..=0xFE, ..])
                | (Self::Gb2312, [0xA1..=0xF7, 0xA1..=0xFE, ..])
                | (
                    Self::Ujis | Self::EucJpMs,
                    [0xA1..=0xFE, 0xA1..=0xFE, ..] | [0x8E, 0xA1..=0xDF, ..]
                )
                | (Self::EucKr, [0x81..=0xFE, 0x41..=0x5A | 0x61..=0x7A | 0x81..=0xFE, ..])
        );
        if wide {
            4
        } else if triple {
            3
        } else if pair {
            2
        } else {
            1
        }
    }

    /// The bytes the server counts a character at the start of `b` to have when `b[0]` leads one, whether or not the rest of it is there.
    fn lead_claim(self, b: &[u8]) -> usize {
        match (self, b) {
            (Self::Gb18030, [0x81..=0xFE, 0x30..=0x39, ..]) => 4,
            (Self::Ujis | Self::EucJpMs, [0x8F, ..]) => 3,
            (Self::Big5, [0xA1..=0xF9, ..])
            | (Self::Sjis | Self::Cp932, [0x81..=0x9F | 0xE0..=0xFC, ..])
            | (Self::Gbk | Self::Gb18030 | Self::EucKr, [0x81..=0xFE, ..])
            | (Self::Gb2312, [0xA1..=0xFE, ..])
            | (Self::Ujis | Self::EucJpMs, [0x8E | 0xA1..=0xFE, ..]) => 2,
            _ => 0,
        }
    }

    /// The character a two-byte pair decodes to by the encoding standard, the marker when it is not one character the lexer may read.
    fn decoded(self, pair: &[u8]) -> char {
        plain(&self.encoding().decode_without_bom_handling(pair).0)
    }

    /// The character the server stores for this pair when it is not the one the encoding standard decodes it to.
    fn fixed(self, pair: &[u8]) -> Option<char> {
        let key = (self.name(), pair[0], pair[1]);
        let at = PAIR_FIXES.partition_point(|&(n, l, lo, ..)| (n, l, lo) <= key);
        let &(n, l, lo, hi, base) = PAIR_FIXES.get(at.checked_sub(1)?)?;
        ((n, l) == (key.0, key.1) && key.2 <= hi).then(|| {
            char::from_u32(base + u32::from(key.2 - lo))
                .filter(|_| base != 0)
                .unwrap_or(UNPLACED)
        })
    }

    /// The character the server stores for this character of the client.
    fn character(self, one: &[u8]) -> char {
        match one {
            [_, _, _] => UNPLACED,
            [_, _] => self.fixed(one).unwrap_or_else(|| self.decoded(one)),
            _ => self.decoded(one),
        }
    }
}

/// The bytes of the identifier that follow a lead which starts no character, if the character it claims does not fit in them: the server drops them from the name.
fn swallowed(rest: &[u8], claim: usize, lex: Lexical) -> usize {
    let run = rest
        .iter()
        .take_while(|&&b| {
            b.is_ascii_alphanumeric()
                || b == b'_'
                || b == b'$'
                || b >= 0x80 && !lex.blank.contains(&b)
        })
        .count();
    if claim > 0 && run < claim { run } else { 0 }
}

fn ends_in_word(out: &str) -> bool {
    out.chars().next_back().is_some_and(word_char)
}

/// The character a byte the reader cannot place is read as: a letter to the lexer, neither a blank nor the start of a comment.
pub(crate) const UNPLACED: char = '\u{FFFD}';

/// What a client collation does to the bytes the lexer meets, as the server reports it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Lexical {
    pub(crate) blank: &'static [u8],
    pub(crate) opener: &'static [u8],
    pub(crate) del_opens: bool,
}

impl Lexical {
    /// The lexing of the collation with this id; an id the table does not list lexes like ASCII.
    pub(crate) fn of_collation(id: Option<u16>) -> Self {
        id.and_then(|id| LEXICAL.binary_search_by_key(&id, |(i, ..)| *i).ok())
            .map_or(
                Self {
                    blank: &[],
                    opener: &[],
                    del_opens: true,
                },
                |at| Self {
                    blank: LEXICAL[at].1,
                    opener: LEXICAL[at].2,
                    del_opens: LEXICAL[at].3,
                },
            )
    }

    /// The character one byte stands for, `after_dashes` being whether `--` precedes it: a blank high byte is a space, a high byte that opens a comment is a control character, a 0x7F that opens none is a letter, any other high byte the marker.
    fn single(self, byte: u8, after_dashes: bool) -> char {
        match byte {
            0x7F if after_dashes && !self.del_opens => UNPLACED,
            0..=0x7F => char::from(byte),
            b if self.blank.contains(&b) => ' ',
            b if after_dashes && self.opener.contains(&b) => '\x01',
            _ => UNPLACED,
        }
    }
}

/// How the server cuts a statement into characters in a client character set.
enum Cut {
    Utf8,
    Utf8Mb4,
    Bytes(&'static str),
    Pairs(Layout),
}

impl Cut {
    /// The cut of the character set a QUERY event's client used (its collation id); the other UTF-8 sets, `binary` and an unrecorded set are read as UTF-8.
    fn of_collation(id: Option<u16>) -> Self {
        match id.map(charset_name) {
            Some(Some("utf8mb4")) => Self::Utf8Mb4,
            None | Some(Some("utf8mb3" | "binary" | "ascii")) => Self::Utf8,
            Some(Some(n)) => Layout::of_name(n).map_or(Self::Bytes(n), Self::Pairs),
            Some(None) => Self::Bytes(""),
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

/// `raw` cut into characters: a pair of `layout` is the character it encodes, every other byte is what `lex` reads it as, and a lead that starts no character inside an identifier is dropped with the rest of the identifier when its character would not fit.
fn cut(raw: &[u8], layout: Option<Layout>, charset: &str, lex: Lexical) -> String {
    let mut out = String::with_capacity(raw.len());
    let mut rest = raw;
    while let Some(&first) = rest.first() {
        let width = layout.map_or(1, |l| l.char_len(rest).max(1));
        let dropped = match layout {
            Some(l) if width == 1 && ends_in_word(&out) => swallowed(rest, l.lead_claim(rest), lex),
            _ => 0,
        };
        if dropped > 0 {
            rest = &rest[dropped..];
            continue;
        }
        let (one, tail) = rest.split_at(width);
        out.push(match layout {
            Some(l) if width >= 2 => l.character(one),
            _ => match lex.single(first, out.ends_with("--")) {
                UNPLACED if first >= 0x80 => high_byte(charset, first),
                c => c,
            },
        });
        rest = tail;
    }
    out
}

/// The character the server stores for a high byte of a character set, the marker when the byte is no character of it or one the lexer may not read as a letter.
fn high_byte(charset: &str, byte: u8) -> char {
    HIGH_BYTES
        .binary_search_by_key(&charset, |(n, _)| n)
        .ok()
        .and_then(|at| HIGH_BYTES[at].1.chars().nth(usize::from(byte - 0x80)))
        .filter(|c| c.is_alphanumeric() || !c.is_ascii() && !c.is_whitespace())
        .unwrap_or(UNPLACED)
}

/// The bytes a UTF-8 lead claims, 0 for a byte that leads nothing.
fn utf8_claim(lead: u8) -> usize {
    match lead {
        0xC2..=0xDF => 2,
        0xE0..=0xEF => 3,
        0xF0..=0xFE => 4,
        _ => 0,
    }
}

/// `raw` as UTF-8 text: an invalid sequence is the marker, except a lead inside an identifier that the server drops with the rest of it.
fn cut_utf8(raw: &[u8]) -> String {
    let mut out = String::with_capacity(raw.len());
    let mut rest = raw;
    while let Err(e) = std::str::from_utf8(rest) {
        let (good, bad) = rest.split_at(e.valid_up_to());
        out.push_str(&String::from_utf8_lossy(good));
        let dropped = if ends_in_word(&out) {
            swallowed(bad, utf8_claim(bad[0]), Lexical::of_collation(None))
        } else {
            0
        };
        if dropped > 0 {
            rest = &bad[dropped..];
        } else {
            out.push(UNPLACED);
            rest = &bad[e.error_len().unwrap_or(bad.len())..];
        }
    }
    out.push_str(&String::from_utf8_lossy(rest));
    out
}

/// The statement without the one NUL that ends it (the server reads it as the end of the text, not as a character).
fn without_final_nul(text: std::borrow::Cow<'_, str>) -> std::borrow::Cow<'_, str> {
    let kept = text.trim_end_matches(|c: char| matches!(c, ' ' | '\t'..='\r' | ';'));
    match kept.strip_suffix('\0') {
        Some(head) => format!("{head}{}", &text[kept.len()..]).into(),
        None => text,
    }
}

/// A statement's bytes as text, cut into characters where the server cuts them in the client character set (its collation id) and with the blanks and comment openers that collation has; UTF-8 and an unrecorded set are read as UTF-8.
pub(crate) fn client_text(raw: &[u8], charset: Option<u16>) -> std::borrow::Cow<'_, str> {
    let lex = Lexical::of_collation(charset);
    without_final_nul(match Cut::of_collation(charset) {
        Cut::Utf8 => String::from_utf8_lossy(raw),
        Cut::Utf8Mb4 => cut_utf8(raw).into(),
        Cut::Bytes(n) => cut(raw, None, n, lex).into(),
        Cut::Pairs(layout) => cut(raw, Some(layout), layout.name(), lex).into(),
    })
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
        assert_eq!(client_text(b"A\x81", Some(28)), "A");
        assert_eq!(client_text(b"A\x81 B", Some(28)), "A B");
        assert_eq!(client_text(b"A\x81\x7F", Some(28)), "A\x7F");
        assert_eq!(client_text(b"A\xA4\x20", Some(1)), "A ");
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
            assert_eq!(client_text(b"\x85\x5C", Some(id)), "\u{FFFD}", "{id}");
            assert_eq!(first_quote_is_closed(b"'\x85\x5C'", id), Some(true), "{id}");
        }
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

    fn row(id: u16) -> Lexical {
        Lexical::of_collation(Some(id))
    }

    fn stored(charset: &str, byte: u8) -> char {
        HIGH_BYTES
            .iter()
            .find(|(n, _)| *n == charset)
            .and_then(|(_, chars)| chars.chars().nth(usize::from(byte - 0x80)))
            .filter(|c| c.is_alphanumeric() || !c.is_ascii() && !c.is_whitespace())
            .unwrap_or(UNPLACED)
    }

    #[test]
    fn every_single_byte_is_read_as_the_collation_table_says() {
        let unclientable = ["ucs2", "utf16", "utf16le", "utf32"];
        let mut cells = 0;
        for (id, name) in COLLATION_CHARSET {
            if unclientable.contains(name) {
                continue;
            }
            let lex = row(*id);
            let utf8 = matches!(*name, "utf8mb3" | "utf8mb4" | "binary" | "ascii");
            for byte in 0..=0xFFu8 {
                let one = [byte];
                let text = client_text(&one, Some(*id));
                let want = match byte {
                    0..=0x7F => char::from(byte),
                    b if lex.blank.contains(&b) && !utf8 => ' ',
                    b => stored(name, b),
                };
                let want = if byte == 0 {
                    String::new()
                } else {
                    want.to_string()
                };
                assert_eq!(text, want, "{name} {byte:#x}");
                let raw = [b"--", &one[..], b"\nx"].concat();
                let dashed = client_text(&raw, Some(*id));
                let opens = matches!(
                    tokens(&dashed, SqlMode::default()).first(),
                    Some(Tok::Blank(_))
                );
                let want_opens = match byte {
                    0..=0x20 => true,
                    0x21..=0x7E => false,
                    0x7F => lex.del_opens,
                    b => !utf8 && (lex.blank.contains(&b) || lex.opener.contains(&b)),
                };
                assert_eq!(opens, want_opens, "{name} {byte:#x}");
                cells += 2;
            }
        }
        assert!(cells > 100_000, "{cells}");
        assert_eq!(
            client_text(b"\x01 a\tb\r\n\x0B\x0C", None),
            "\x01 a\tb\r\n\x0B\x0C"
        );
        assert_eq!(client_text(b"a\x1Fb", Some(255)), "a\x1Fb");
        assert_eq!(client_text(b"a\x7Fb", Some(255)), "a\x7Fb");
        assert_eq!(client_text(b"caf\xE9", Some(8)), "caf\u{E9}");
        assert_eq!(client_text(b"caf\xA0t", Some(8)), "caf t");
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
    fn the_one_nul_that_ends_a_statement_is_not_part_of_it() {
        for (raw, want) in [
            (&b"TRUNCATE t\0"[..], "TRUNCATE t"),
            (b"TRUNCATE t \0", "TRUNCATE t "),
            (b"TRUNCATE t\0 ;", "TRUNCATE t ;"),
            (b"TRUNCATE t\0\0", "TRUNCATE t\0"),
            (b"TRUNCATE t\0x", "TRUNCATE t\0x"),
            (b"TRUNCATE t\0 \0", "TRUNCATE t\0 "),
            (b"\0", ""),
        ] {
            for id in [None, Some(8), Some(255), Some(13)] {
                assert_eq!(client_text(raw, id), want, "{raw:?} {id:?}");
            }
        }
    }

    #[test]
    fn a_lead_that_starts_no_character_inside_a_utf8mb4_name_is_dropped_when_its_character_does_not_fit()
     {
        for (raw, want) in [
            (&b"zq\xE0"[..], "zq"),
            (b"zq\xC3", "zq"),
            (b"zq\xF0\x9F", "zq"),
            (b"zq\xE0a b", "zq b"),
            (b"zq\xE0abc", "zq\u{FFFD}abc"),
            (b"zq\xC3\xA9", "zq\u{E9}"),
            (b" \xE0", " \u{FFFD}"),
            (b"zq\xFF", "zq\u{FFFD}"),
            (b"zq\x80", "zq\u{FFFD}"),
        ] {
            assert_eq!(client_text(raw, Some(255)), want, "{raw:?}");
        }
        for id in [33, 11, 63, 83, 192] {
            assert_eq!(client_text(b"zq\xE0", Some(id)), "zq\u{FFFD}", "{id}");
        }
    }

    #[test]
    fn a_lead_that_starts_no_character_inside_a_name_is_dropped_in_each_pair_layout() {
        for (name, raw, want) in [
            ("sjis", &b"A\x81"[..], "A"),
            ("sjis", b"A\x81 B", "A B"),
            ("sjis", b"A\x81A", "A\u{3001}"),
            ("gbk", b"A\x81", "A"),
            ("big5", b"A\xA4", "A"),
            ("ujis", b"A\x8F", "A"),
            ("ujis", b"A\x8Fbc", "A\u{FFFD}bc"),
            ("eucjpms", b"A\x8Fb", "A"),
            ("euckr", b"A\x81", "A"),
            ("gb2312", b"A\xA1", "A"),
            ("cp932", b"A\x81", "A"),
            ("gb18030", b"A\x81\x30", "A"),
            ("sjis", b" \x81", " \u{FFFD}"),
        ] {
            assert_eq!(
                client_text(raw, Some(default_id(name))),
                want,
                "{name} {raw:?}"
            );
        }
    }

    #[test]
    fn a_name_in_each_pair_layout_is_decoded_to_the_text_the_server_stores() {
        for (name, raw, want) in [
            ("ujis", &b"\xA4\xA2"[..], "\u{3042}"),
            ("ujis", b"\x8E\xB1", "\u{FF71}"),
            ("ujis", b"\x8F\xB0\xA1", "\u{FFFD}"),
            ("eucjpms", b"\xA4\xA2", "\u{3042}"),
            ("euckr", b"\xB0\xA1", "\u{AC00}"),
            ("gb2312", b"\xB0\xA1", "\u{554A}"),
            ("cp932", b"\x82\xA0", "\u{3042}"),
            ("sjis", b"\x82\xA0", "\u{3042}"),
            ("gbk", b"\xB0\xA1", "\u{554A}"),
            ("big5", b"\xA4\x40", "\u{4E00}"),
            ("big5", b"\xC7\xB1", "\u{414}"),
            ("big5", b"\xC7\xB3", "\u{401}"),
        ] {
            assert_eq!(
                client_text(raw, Some(default_id(name))),
                want,
                "{name} {raw:?}"
            );
        }
    }

    #[test]
    fn a_single_byte_name_is_decoded_to_the_text_the_server_stores() {
        for (name, raw, want) in [
            ("latin1", &b"caf\xE9"[..], "caf\u{E9}"),
            ("latin1", b"caf\xA0t", "caf t"),
            ("latin2", b"\xB1", "\u{105}"),
            ("cp1251", b"\xC0", "\u{410}"),
            ("koi8r", b"\xC1", "\u{430}"),
        ] {
            assert_eq!(
                client_text(raw, Some(default_id(name))),
                want,
                "{name} {raw:?}"
            );
        }
    }

    #[test]
    fn a_statement_of_ascii_or_valid_utf8_is_read_the_same_in_every_client_collation() {
        for (id, name) in COLLATION_CHARSET {
            if ["ucs2", "utf16", "utf16le", "utf32"].contains(name) {
                continue;
            }
            for raw in [
                &b"ALTER TABLE t CHANGE a b INT; -- x"[..],
                b"TRUNCATE `t`",
                b"DROP TABLE a.b, c.d",
            ] {
                assert_eq!(
                    client_text(raw, Some(*id)),
                    String::from_utf8_lossy(raw),
                    "{id} {name}"
                );
            }
        }
        for id in [33, 45, 46, 255] {
            assert_eq!(
                client_text("TRUNCATE caf\u{E9}".as_bytes(), Some(id)),
                "TRUNCATE caf\u{E9}",
                "{id}"
            );
        }
    }

    #[test]
    #[ignore = "feeds dev/pytools/mysql_client_charsets.py"]
    fn dump_pair_decodes() {
        for (name, layout) in LAYOUTS {
            for lead in 0x80..=0xFFu8 {
                for trail in 0x00..=0xFFu8 {
                    let pair = [lead, trail];
                    if layout.char_len(&pair) == 2 {
                        let read = match layout.decoded(&pair) {
                            UNPLACED => "-".to_string(),
                            c => format!("{:X}", u32::from(c)),
                        };
                        println!("PAIR {name} {lead:02X}{trail:02X} {read}");
                    }
                }
            }
        }
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
