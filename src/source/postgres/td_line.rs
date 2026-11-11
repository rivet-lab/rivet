//! The one reader of a `test_decoding` data line: header, operation, then `name[type]:value` cells.

/// What a `test_decoding` table line does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LineOp {
    Insert,
    Update,
    Delete,
}

/// One cell as the decoder printed it: name and type unquoted, value unescaped, `quoted` whether the value carried `'` quotes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RawColumn {
    pub name: String,
    pub typ: String,
    pub value: String,
    pub quoted: bool,
}

/// One `table <relations>: <OP>: <cells>` line cut once: the relation list, the operation, the old-key cells and the new cells.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Line<'a> {
    pub relations: &'a str,
    pub op: LineOp,
    pub old: Option<Vec<RawColumn>>,
    pub new: Vec<RawColumn>,
}

/// The relation list and the rest of a `table <relations>: <rest>` line, cut at the first `: ` outside quotes.
fn table_head(data: &str) -> Option<(&str, &str)> {
    let rest = data.strip_prefix("table ")?;
    let sep = find_outside_quotes(rest, ": ")?;
    Some((&rest[..sep], &rest[sep + 2..]))
}

/// The relation list of a TRUNCATE line; `None` for any other line.
pub(crate) fn truncate_relations(data: &str) -> Option<&str> {
    let (relations, tail) = table_head(data)?;
    tail.starts_with("TRUNCATE:").then_some(relations)
}

/// Cut a `test_decoding` line; `None` for anything that is not an INSERT, UPDATE or DELETE table line.
pub(crate) fn tokenize(data: &str) -> Option<Line<'_>> {
    let (relations, tail) = table_head(data)?;
    let (op, after_op) = [
        ("INSERT", LineOp::Insert),
        ("UPDATE", LineOp::Update),
        ("DELETE", LineOp::Delete),
    ]
    .into_iter()
    .find_map(|(word, op)| tail.strip_prefix(word).map(|r| (op, r)))?;
    let body = after_op.strip_prefix(": ").unwrap_or("");
    let (old, new) = match body.strip_prefix("old-key: ") {
        Some(old_section) => {
            let (first, remainder) = columns(old_section);
            match remainder.strip_prefix("new-tuple: ") {
                Some(new_section) => (Some(first), columns(new_section).0),
                None => (None, first),
            }
        }
        None => (None, columns(body).0),
    };
    Some(Line {
        relations,
        op,
        old,
        new,
    })
}

/// Read cells from the start of `s` until one cannot be read; returns them and what is left.
pub(crate) fn columns(s: &str) -> (Vec<RawColumn>, &str) {
    let mut out = Vec::new();
    let mut rest = s.trim_start();
    while let Some((column, after)) = column(rest) {
        out.push(column);
        rest = after.trim_start();
    }
    (out, rest)
}

/// One `name[type]:value` cell at the start of `s`, with what follows it.
fn column(s: &str) -> Option<(RawColumn, &str)> {
    let (name, after_name) = read_name(s)?;
    let after_open = after_name.strip_prefix('[')?;
    let type_end = type_end(after_open)?;
    let typ = after_open[..type_end].to_string();
    let (value, quoted, consumed) = parse_value(&after_open[type_end + 2..]);
    Some((
        RawColumn {
            name,
            typ,
            value,
            quoted,
        },
        &after_open[type_end + 2 + consumed..],
    ))
}

/// A column name at the start of `s`: a quoted identifier to its closing quote, or a bare `[A-Za-z0-9_]` run; followed by the `[` of its type.
fn read_name(s: &str) -> Option<(String, &str)> {
    if let Some(quoted) = s.strip_prefix('"') {
        let end = closing_quote(quoted)?;
        let after = &quoted[end + 1..];
        return after
            .starts_with('[')
            .then(|| (quoted[..end].replace("\"\"", "\""), after));
    }
    let end = s
        .bytes()
        .position(|b| !(b.is_ascii_alphanumeric() || b == b'_'))
        .unwrap_or(s.len());
    (end > 0 && s[end..].starts_with('[')).then(|| (s[..end].to_string(), &s[end..]))
}

/// The index of the quote that closes an identifier whose opening quote was consumed, `""` being an escaped quote.
fn closing_quote(s: &str) -> Option<usize> {
    let b = s.as_bytes();
    let mut skip = false;
    for (i, &c) in b.iter().enumerate() {
        if skip {
            skip = false;
        } else if c == b'"' {
            if b.get(i + 1) == Some(&b'"') {
                skip = true;
            } else {
                return Some(i);
            }
        }
    }
    None
}

/// The index of the `]` that ends a type label, the first `]:` outside a quoted identifier.
fn type_end(s: &str) -> Option<usize> {
    let b = s.as_bytes();
    let mut in_quotes = false;
    let mut skip = false;
    for (i, &c) in b.iter().enumerate() {
        if skip {
            skip = false;
            continue;
        }
        match c {
            b'"' if in_quotes && b.get(i + 1) == Some(&b'"') => skip = true,
            b'"' => in_quotes = !in_quotes,
            b']' if !in_quotes && b.get(i + 1) == Some(&b':') => return Some(i),
            _ => {}
        }
    }
    None
}

/// Parse one value at the start of `s`; returns `(value, quoted, bytes_consumed)`.
fn parse_value(s: &str) -> (String, bool, usize) {
    let b = s.as_bytes();
    if b.first() != Some(&b'\'') {
        let end = s.find(' ').unwrap_or(s.len());
        return (s[..end].to_string(), false, end);
    }
    let mut v = String::new();
    let mut i = 1;
    while i < b.len() {
        if b[i] == b'\'' {
            if b.get(i + 1) == Some(&b'\'') {
                v.push('\'');
                i += 2;
            } else {
                return (v, true, i + 1);
            }
        } else {
            let n = utf8_len(b[i]);
            v.push_str(s.get(i..i + n).unwrap_or_default());
            i += n;
        }
    }
    (v, true, i)
}

/// The byte length of the UTF-8 character that starts with `lead`.
pub(crate) fn utf8_len(lead: u8) -> usize {
    match lead {
        b if b < 0x80 => 1,
        b if b >> 5 == 0b110 => 2,
        b if b >> 4 == 0b1110 => 3,
        _ => 4,
    }
}

/// The byte offset of `needle` in `haystack` outside any `'` literal and any `"` identifier, `''` and `""` being escaped quotes.
pub(crate) fn find_outside_quotes(haystack: &str, needle: &str) -> Option<usize> {
    #[derive(PartialEq)]
    enum In {
        Nothing,
        Literal,
        Ident,
    }
    let (b, nb) = (haystack.as_bytes(), needle.as_bytes());
    let mut i = 0;
    let mut state = In::Nothing;
    while i < b.len() {
        match state {
            In::Literal => {
                if b[i] == b'\'' {
                    if b.get(i + 1) == Some(&b'\'') {
                        i += 2;
                        continue;
                    }
                    state = In::Nothing;
                }
                i += 1;
            }
            In::Ident => {
                if b[i] == b'"' {
                    if b.get(i + 1) == Some(&b'"') {
                        i += 2;
                        continue;
                    }
                    state = In::Nothing;
                }
                i += 1;
            }
            In::Nothing => {
                if b[i] == b'\'' {
                    state = In::Literal;
                    i += 1;
                } else if b[i] == b'"' {
                    state = In::Ident;
                    i += 1;
                } else if b[i..].starts_with(nb) {
                    return Some(i);
                } else {
                    i += 1;
                }
            }
        }
    }
    None
}

/// An identifier as the decoder prints it, unquoted: `"a""b"` is `a"b`, a bare name is itself.
pub(crate) fn unquote_ident(raw: &str) -> String {
    let t = raw.trim();
    match t.strip_prefix('"').and_then(|r| r.strip_suffix('"')) {
        Some(inner) => inner.replace("\"\"", "\""),
        _ => t.to_string(),
    }
}

/// Split `schema.table` where either part may be quoted and a quoted part may hold a dot.
pub(crate) fn split_qualified_ident(qual: &str) -> (String, String) {
    let mut in_quotes = false;
    let mut escaped = false;
    let mut split_at = None;
    for (i, b) in qual.bytes().enumerate() {
        if escaped {
            escaped = false;
            continue;
        }
        match b {
            b'"' if in_quotes && qual.as_bytes().get(i + 1) == Some(&b'"') => {
                escaped = true;
            }
            b'"' => in_quotes = !in_quotes,
            b'.' if !in_quotes => {
                split_at = Some(i);
                break;
            }
            _ => {}
        }
    }
    match split_at {
        Some(i) => (unquote_ident(&qual[..i]), unquote_ident(&qual[i + 1..])),
        None => (String::new(), unquote_ident(qual)),
    }
}

/// Split a relation list on top-level commas; a comma inside a quoted identifier is part of the name.
pub(crate) fn split_top_level_commas(list: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let mut in_quotes = false;
    let mut escaped = false;
    let mut start = 0usize;
    let b = list.as_bytes();
    for i in 0..b.len() {
        if escaped {
            escaped = false;
            continue;
        }
        match b[i] {
            b'"' if in_quotes && b.get(i + 1) == Some(&b'"') => escaped = true,
            b'"' => in_quotes = !in_quotes,
            b',' if !in_quotes => {
                out.push(&list[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    out.push(&list[start..]);
    out
}

/// The `(head, content)` of a `message: ` line, cut at its ` content:` marker.
pub(crate) fn message_parts(line: &str) -> Option<(&str, &str)> {
    line.strip_prefix("message: ")?.split_once(" content:")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cell(name: &str, typ: &str, value: &str, quoted: bool) -> RawColumn {
        RawColumn {
            name: name.into(),
            typ: typ.into(),
            value: value.into(),
            quoted,
        }
    }

    /// A relation name holding `: ` is part of the relation, not the end of the header.
    #[test]
    fn a_relation_holding_colon_space_is_cut_at_the_operation() {
        let line = tokenize("table \"s: x\".orders: INSERT: id[integer]:1 v[text]:'a'").unwrap();
        assert_eq!(line.relations, "\"s: x\".orders");
        assert_eq!(line.op, LineOp::Insert);
        assert_eq!(
            line.new,
            vec![
                cell("id", "integer", "1", false),
                cell("v", "text", "a", true)
            ]
        );
    }

    /// Column names holding `[`, `]:`, `: `, quotes and spaces come back as the names the table has.
    #[test]
    fn hostile_column_names_come_back_whole() {
        let line = tokenize(
            "table public.t: INSERT: \"k[1\"[integer]:3 \"a[b]:c\"[text]:'zz' \"x]: y\"[integer]:12 \
             \"q\"\"r'\"[text]:'p' plain[text]:'v'",
        )
        .unwrap();
        let got: Vec<(&str, &str)> = line
            .new
            .iter()
            .map(|c| (c.name.as_str(), c.value.as_str()))
            .collect();
        assert_eq!(
            got,
            vec![
                ("k[1", "3"),
                ("a[b]:c", "zz"),
                ("x]: y", "12"),
                ("q\"r'", "p"),
                ("plain", "v")
            ]
        );
    }

    /// An array type label carries its own `[]`, and a quoted type name may hold `]:`; the value follows the first `]:` outside quotes.
    #[test]
    fn a_type_label_is_read_to_its_own_end() {
        let line = tokenize(
            "table public.t: INSERT: a[integer[]]:'{1,2}' b[\"My]: Type\"]:'x' c[\"s: x\".mood]:'ok'",
        )
        .unwrap();
        let types: Vec<&str> = line.new.iter().map(|c| c.typ.as_str()).collect();
        assert_eq!(types, vec!["integer[]", "\"My]: Type\"", "\"s: x\".mood"]);
        let values: Vec<&str> = line.new.iter().map(|c| c.value.as_str()).collect();
        assert_eq!(values, vec!["{1,2}", "x", "ok"]);
    }

    /// An UPDATE that changes the key prints both sections; each keeps its own cells, whatever the values hold.
    #[test]
    fn old_key_and_new_tuple_are_separate_sections() {
        let line = tokenize(
            "table public.t: UPDATE: old-key: id[integer]:1 n[text]:'a new-tuple: b' \
             new-tuple: id[integer]:2 n[text]:'c'",
        )
        .unwrap();
        assert_eq!(line.op, LineOp::Update);
        assert_eq!(
            line.old.unwrap(),
            vec![
                cell("id", "integer", "1", false),
                cell("n", "text", "a new-tuple: b", true)
            ]
        );
        assert_eq!(
            line.new,
            vec![
                cell("id", "integer", "2", false),
                cell("n", "text", "c", true)
            ]
        );
    }

    /// An old-key section with no new-tuple after it is the new image, as before.
    #[test]
    fn an_old_key_without_a_new_tuple_is_read_as_the_new_image() {
        let line = tokenize("table public.t: UPDATE: old-key: id[integer]:1").unwrap();
        assert_eq!(line.old, None);
        assert_eq!(line.new, vec![cell("id", "integer", "1", false)]);
    }

    /// The operations without cells, the markers and non-table lines all read as no cells or no line.
    #[test]
    fn lines_without_cells_and_foreign_lines() {
        let delete = tokenize("table public.t: DELETE: (no-tuple-data)").unwrap();
        assert_eq!(
            (delete.op, delete.old, delete.new),
            (LineOp::Delete, None, vec![])
        );
        for line in [
            "BEGIN 123",
            "COMMIT 123",
            "message: transactional: 0 prefix: p, sz: 1 content:x",
            "table public.t: TRUNCATE: (no-flags)",
            "table public.t",
            "table public.t: MERGE: id[integer]:1",
            "",
        ] {
            assert_eq!(tokenize(line), None, "{line:?}");
        }
    }

    /// TRUNCATE names its relations through the same header cut, whatever the names hold.
    #[test]
    fn truncate_relations_are_cut_at_the_operation() {
        assert_eq!(
            truncate_relations("table public.a, \"s: x\".\"o'brien\": TRUNCATE: (no-flags)"),
            Some("public.a, \"s: x\".\"o'brien\"")
        );
        assert_eq!(
            truncate_relations("table public.t: INSERT: id[integer]:1"),
            None
        );
        assert_eq!(truncate_relations("BEGIN 1"), None);
    }

    /// A malformed tail ends the cells where it stops being a cell and keeps the ones before it.
    #[test]
    fn a_malformed_tail_keeps_the_cells_before_it() {
        let (cells, rest) = columns("a[integer]:1 b[text 'x' c");
        assert_eq!(cells, vec![cell("a", "integer", "1", false)]);
        assert_eq!(rest, "b[text 'x' c");
        let (cells, rest) = columns("\"unterminated[integer]:1");
        assert!(cells.is_empty());
        assert_eq!(rest, "\"unterminated[integer]:1");
    }

    /// The message line is cut at its first ` content:`.
    #[test]
    fn a_message_line_is_cut_at_its_content_marker() {
        assert_eq!(
            message_parts("message: transactional: 0 prefix: rivet, sz: 3 content:abc content:d"),
            Some(("transactional: 0 prefix: rivet, sz: 3", "abc content:d"))
        );
        assert_eq!(message_parts("message: no marker here"), None);
        assert_eq!(message_parts("table public.t: INSERT: x"), None);
    }

    /// Wall-clock ceiling for one hostile megabyte: one pass needs milliseconds, a quadratic one minutes.
    const HOSTILE_BUDGET: std::time::Duration = std::time::Duration::from_secs(10);

    /// `tokenize` of `line` on a watchdog thread, so an overrun fails the test instead of hanging it.
    fn tokenize_in_budget(label: &str, line: String) {
        let (done, result) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let _ = tokenize(&line);
            let _ = truncate_relations(&line);
            let _ = done.send(());
        });
        result
            .recv_timeout(HOSTILE_BUDGET)
            .unwrap_or_else(|why| panic!("SCAN NOT LINEAR: {label}: {why} in {HOSTILE_BUDGET:?}"));
    }

    /// One megabyte of each hostile shape of a line is read in one pass, inside the budget.
    #[test]
    fn the_tokenizer_is_linear_on_a_hostile_megabyte() {
        const MB: usize = 1 << 20;
        let head = "table public.t: INSERT: ";
        let cases = [
            ("open brackets", format!("{head}{}", "a[".repeat(MB / 2))),
            ("type colons", format!("{head}a[{}", "]x".repeat(MB / 2))),
            ("quotes", format!("{head}{}", "\"".repeat(MB))),
            ("name quotes", format!("{head}{}", "\"a\"[".repeat(MB / 4))),
            ("literal quotes", format!("{head}a[t]:{}", "'".repeat(MB))),
            (
                "doubled quotes",
                format!("{head}a[t]:'{}", "''".repeat(MB / 2)),
            ),
            ("colon spaces", format!("table {}", ": ".repeat(MB / 2))),
            (
                "quoted colon spaces",
                format!("table \"{}", ": ".repeat(MB / 2)),
            ),
            ("cells", format!("{head}{}", "a[t]:1 ".repeat(MB / 7))),
            (
                "old keys",
                format!("{head}old-key: {}", "a[t]:1 ".repeat(MB / 7)),
            ),
        ];
        for (label, line) in cases {
            tokenize_in_budget(label, line);
        }
    }
}
