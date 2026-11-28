//! Whether a text is one RFC 8259 JSON value, read without recursion so no depth or number range refuses a valid document.

/// Whether `text` is exactly one JSON value, with any whitespace around it.
pub(crate) fn is_json_text(text: &str) -> bool {
    let b = text.as_bytes();
    let mut i = 0;
    let mut open: Vec<u8> = Vec::new();
    loop {
        match value_start(b, &mut i, &mut open) {
            Start::Bad => return false,
            Start::Opened => continue,
            Start::Done => {}
        }
        loop {
            skip_ws(b, &mut i);
            match (open.last().copied(), b.get(i).copied()) {
                (None, None) => return true,
                (Some(close), Some(c)) if c == close => {
                    i += 1;
                    open.pop();
                }
                (Some(b'}'), Some(b',')) => {
                    i += 1;
                    if !member_key(b, &mut i) {
                        return false;
                    }
                    break;
                }
                (Some(b']'), Some(b',')) => {
                    i += 1;
                    break;
                }
                _ => return false,
            }
        }
    }
}

enum Start {
    Done,
    Opened,
    Bad,
}

impl From<bool> for Start {
    fn from(read: bool) -> Self {
        if read { Start::Done } else { Start::Bad }
    }
}

/// Reads one scalar or an empty container whole, or only the opening of a container (pushing its closer, and an object's first key).
fn value_start(b: &[u8], i: &mut usize, open: &mut Vec<u8>) -> Start {
    skip_ws(b, i);
    let Some(&c) = b.get(*i) else {
        return Start::Bad;
    };
    match c {
        b'{' | b'[' => {
            *i += 1;
            skip_ws(b, i);
            let close = if c == b'{' { b'}' } else { b']' };
            if b.get(*i) == Some(&close) {
                *i += 1;
                return Start::Done;
            }
            open.push(close);
            Start::from(c == b'[' || member_key(b, i)).opened()
        }
        b'"' => string(b, i).into(),
        b'-' | b'0'..=b'9' => number(b, i).into(),
        b't' => literal(b, i, b"true").into(),
        b'f' => literal(b, i, b"false").into(),
        b'n' => literal(b, i, b"null").into(),
        _ => Start::Bad,
    }
}

impl Start {
    fn opened(self) -> Start {
        match self {
            Start::Done => Start::Opened,
            other => other,
        }
    }
}

fn skip_ws(b: &[u8], i: &mut usize) {
    while matches!(b.get(*i), Some(b' ' | b'\t' | b'\n' | b'\r')) {
        *i += 1;
    }
}

fn literal(b: &[u8], i: &mut usize, word: &[u8]) -> bool {
    let hit = b[*i..].starts_with(word);
    if hit {
        *i += word.len();
    }
    hit
}

/// An object member's `"name" :`.
fn member_key(b: &[u8], i: &mut usize) -> bool {
    skip_ws(b, i);
    if b.get(*i) != Some(&b'"') || !string(b, i) {
        return false;
    }
    skip_ws(b, i);
    let colon = b.get(*i) == Some(&b':');
    *i += usize::from(colon);
    colon
}

fn digits(b: &[u8], i: &mut usize) -> usize {
    let start = *i;
    while matches!(b.get(*i), Some(b'0'..=b'9')) {
        *i += 1;
    }
    *i - start
}

fn number(b: &[u8], i: &mut usize) -> bool {
    if b.get(*i) == Some(&b'-') {
        *i += 1;
    }
    match b.get(*i) {
        Some(b'0') => *i += 1,
        Some(b'1'..=b'9') => {
            digits(b, i);
        }
        _ => return false,
    }
    if b.get(*i) == Some(&b'.') {
        *i += 1;
        if digits(b, i) == 0 {
            return false;
        }
    }
    if matches!(b.get(*i), Some(b'e' | b'E')) {
        *i += 1;
        if matches!(b.get(*i), Some(b'+' | b'-')) {
            *i += 1;
        }
        if digits(b, i) == 0 {
            return false;
        }
    }
    true
}

fn hex4(b: &[u8], i: &mut usize) -> Option<u32> {
    let h = b.get(*i..*i + 4)?;
    if !h.iter().all(u8::is_ascii_hexdigit) {
        return None;
    }
    let n = u32::from_str_radix(std::str::from_utf8(h).ok()?, 16).ok()?;
    *i += 4;
    Some(n)
}

/// A `\u` escape's code unit; a high surrogate must be followed by a low one, and a low one never stands alone.
fn unicode_escape(b: &[u8], i: &mut usize) -> bool {
    match hex4(b, i) {
        Some(0xD800..=0xDBFF) => {
            b[*i..].starts_with(b"\\u") && {
                *i += 2;
                matches!(hex4(b, i), Some(0xDC00..=0xDFFF))
            }
        }
        Some(0xDC00..=0xDFFF) | None => false,
        Some(_) => true,
    }
}

fn string(b: &[u8], i: &mut usize) -> bool {
    *i += 1;
    loop {
        let Some(&c) = b.get(*i) else {
            return false;
        };
        *i += 1;
        match c {
            b'"' => return true,
            0..=0x1f => return false,
            b'\\' => {
                let Some(&e) = b.get(*i) else {
                    return false;
                };
                *i += 1;
                let ok = match e {
                    b'"' | b'\\' | b'/' | b'b' | b'f' | b'n' | b'r' | b't' => true,
                    b'u' => unicode_escape(b, i),
                    _ => false,
                };
                if !ok {
                    return false;
                }
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::is_json_text;
    use proptest::prelude::*;

    fn serde_says(text: &str) -> bool {
        serde_json::from_str::<serde_json::Value>(text).is_ok()
    }

    #[test]
    fn the_rfc_8259_corner_cases_are_read_as_serde_json_reads_them() {
        for ok in [
            "{}",
            "[]",
            "0",
            "-0",
            "1e5",
            "1E-5",
            "-1.5e+3",
            "\"\"",
            "\"\\u00e9\\ud83d\\ude00\\n\\/\"",
            "true",
            "null",
            " \t\r\n[1 , {\"a\" : [ ] } ]\n",
            "[[[[[[]]]]]]",
            "\"\u{e9}\u{65e5}\"",
        ] {
            assert!(is_json_text(ok), "{ok:?}");
            assert!(serde_says(ok), "{ok:?} (the oracle)");
        }
        for bad in [
            "",
            " ",
            "abc",
            "01",
            "-",
            "1.",
            ".5",
            "+1",
            "1e",
            "NaN",
            "Infinity",
            "tru",
            "nulll",
            "[1,]",
            "[,1]",
            "{\"a\":1,}",
            "{'a':1}",
            "{a:1}",
            "{\"a\" 1}",
            "{\"a\":}",
            "{\"a\":1}x",
            "[1] [2]",
            "\"a",
            "\"a\nb\"",
            "\"\\x\"",
            "\"\\u12\"",
            "\"\\ud800\"",
            "\"\\udc00\"",
            "\"\\ud800\\u0041\"",
            "\u{1}",
            "[",
            "]",
            "{",
            "0102",
        ] {
            assert!(!is_json_text(bad), "{bad:?}");
            assert!(!serde_says(bad), "{bad:?} (the oracle)");
        }
    }

    #[test]
    fn a_document_deeper_than_serde_json_reads_and_a_number_past_f64_are_json() {
        let deep = format!("{}{}", "[".repeat(100_000), "]".repeat(100_000));
        assert!(is_json_text(&deep));
        assert!(!is_json_text(&deep[1..]));
        assert!(is_json_text("1e999"));
        assert!(is_json_text("[-1e999, 123456789012345678901234567890]"));
    }

    fn token() -> impl Strategy<Value = &'static str> {
        prop::sample::select(vec![
            "{",
            "}",
            "[",
            "]",
            ",",
            ":",
            " ",
            "\n",
            "\"",
            "\"a\"",
            "\"\\u00e9\"",
            "\"\\ud83d\"",
            "\"\\ude00\"",
            "\"\\n\"",
            "\"\\q\"",
            "0",
            "1",
            "-",
            "-1",
            "01",
            "1.5",
            "1.",
            "e5",
            "1e5",
            "true",
            "false",
            "null",
            "nul",
            "x",
            "'",
            "\u{1}",
            "\u{e9}",
        ])
    }

    proptest! {
        #[test]
        fn it_agrees_with_serde_json_on_token_soup(tokens in prop::collection::vec(token(), 0..14)) {
            let text = tokens.concat();
            prop_assert_eq!(is_json_text(&text), serde_says(&text), "{:?}", text);
        }

        #[test]
        fn it_agrees_with_serde_json_on_any_text(text in "\\PC{0,24}") {
            prop_assert_eq!(is_json_text(&text), serde_says(&text), "{:?}", text);
        }

        #[test]
        fn every_value_serde_json_writes_is_json(
            nums in prop::collection::vec(any::<i64>(), 0..6),
            word in "\\PC{0,8}",
        ) {
            let v = serde_json::json!({"n": nums, "w": word, "o": {"k": [null, true, 1.5]}});
            prop_assert!(is_json_text(&v.to_string()));
            prop_assert!(is_json_text(&serde_json::to_string_pretty(&v).unwrap()));
        }
    }
}
