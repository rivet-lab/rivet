//! One reading of a MySQL statement as the server lexes it: quotes, comments and executable comments.

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

const VERSION_DIGITS: usize = 5;

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
            let digits = b[i + 3..]
                .iter()
                .take(VERSION_DIGITS)
                .take_while(|d| d.is_ascii_digit())
                .count();
            out.push(Tok::Blank(" "));
            i += 3 + if digits == VERSION_DIGITS { digits } else { 0 };
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
    fn a_version_comment_takes_exactly_five_digits_after_the_bang() {
        for (sql, want) in [
            ("/*!80000 REVOKE */", "REVOKE"),
            ("/*!800001 x */", "1 x"),
            ("/*!8000012345 x */", "12345 x"),
            ("/*!8000 x */", "8000 x"),
            ("/*!x */", "x"),
            ("/*! x */", "x"),
            ("/*!5000a x */", "5000a x"),
            ("/*!80000REVOKE */", "REVOKE"),
        ] {
            assert_eq!(read(sql), want, "{sql:?}");
        }
    }
}
