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

fn word_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_' || c == '$' || !c.is_ascii() && !c.is_whitespace()
}

fn char_at(sql: &str, i: usize) -> Option<char> {
    sql.get(i..).and_then(|s| s.chars().next())
}

/// Where the quote opened at `start` ends (one past its closer, or the end of the text) and whether it was closed.
fn quote_end(sql: &str, start: usize, quote: char) -> (usize, bool) {
    let (b, q) = (sql.as_bytes(), quote as u8);
    let mut j = start + 1;
    while j < b.len() {
        match b[j] {
            b'\\' if quote != '`' => j += 2,
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
pub(crate) fn tokens(sql: &str) -> Vec<Tok<'_>> {
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
            let digits = b[i + 3..].iter().take_while(|d| d.is_ascii_digit()).count();
            out.push(Tok::Blank(" "));
            i += 3 + digits;
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
            let (end, closed) = quote_end(sql, i, c);
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
pub(crate) fn executable_text(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len());
    tokens(sql).iter().for_each(|t| t.raw(&mut out));
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn read(sql: &str) -> String {
        executable_text(sql)
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
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
        let name = |sql: &str| tokens(sql).into_iter().find_map(|t| t.identifier());
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
        let toks = tokens("x 'a -- b\nc");
        assert!(matches!(
            toks.last(),
            Some(Tok::Quoted {
                closed: false,
                quote: '\'',
                ..
            })
        ));
        assert_eq!(executable_text("x 'a -- b\nc"), "x 'a -- b\nc");
    }
}
