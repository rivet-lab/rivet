//! ADR 0040 D5: the call-site shapes an owner seam replaces may only shrink, each counted by a structural read of `src/`.

use crate::error_code_ratchet::product_sources;
use crate::site_ledger::{enclosing_fn, hold, hold_none, number, squash};

/// The words a config field name ends in when it holds a credential or a URL that can carry one.
const CREDENTIAL_WORDS: &[&str] = &[
    "password",
    "passwd",
    "pwd",
    "token",
    "secret",
    "key",
    "credential",
    "credentials",
    "dsn",
    "url",
    "uri",
];

/// `path` relative to the crate root, for a failure line.
fn rel(path: &std::path::Path) -> String {
    path.strip_prefix(env!("CARGO_MANIFEST_DIR"))
        .unwrap_or(path)
        .display()
        .to_string()
}

/// The offset of the `{` that opens a match body after its scrutinee, `None` when a `;` comes first.
fn block_open(b: &[u8], from: usize) -> Option<usize> {
    let mut depth = 0i32;
    for (i, c) in b.iter().enumerate().skip(from) {
        match c {
            b'(' | b'[' => depth += 1,
            b')' | b']' => depth -= 1,
            b'{' if depth == 0 => return Some(i),
            b';' if depth == 0 => return None,
            _ => {}
        }
    }
    None
}

/// Whether `seg` closes a bracket it did not open.
fn unbalanced(seg: &[u8]) -> bool {
    let mut depth = 0i32;
    for c in seg {
        match c {
            b'(' | b'[' | b'{' => depth += 1,
            b')' | b']' | b'}' => {
                depth -= 1;
                if depth < 0 {
                    return true;
                }
            }
            _ => {}
        }
    }
    false
}

/// (end of the arm body that starts at `from`, offset where the next arm starts).
fn arm_end(b: &[u8], from: usize) -> (usize, usize) {
    let start = from
        + b[from..]
            .iter()
            .take_while(|c| c.is_ascii_whitespace())
            .count();
    let block = b.get(start) == Some(&b'{');
    let mut depth = 0i32;
    for (i, c) in b.iter().enumerate().skip(start) {
        match c {
            b'(' | b'[' | b'{' => depth += 1,
            b')' | b']' | b'}' => {
                if depth == 0 {
                    return (i, i);
                }
                depth -= 1;
                if block && depth == 0 {
                    let end = i + 1;
                    let rest = end
                        + b[end..]
                            .iter()
                            .take_while(|c| c.is_ascii_whitespace())
                            .count();
                    return (
                        end,
                        if b.get(rest) == Some(&b',') {
                            rest + 1
                        } else {
                            end
                        },
                    );
                }
            }
            b',' if depth == 0 => return (i, i + 1),
            _ => {}
        }
    }
    (b.len(), b.len())
}

/// Every arm of every `match` in `code`, as (offset of its `=>`, pattern, body).
fn match_arms(code: &str) -> Vec<(usize, String, String)> {
    let b = code.as_bytes();
    let ident = |i: usize| {
        b.get(i)
            .is_some_and(|c| c.is_ascii_alphanumeric() || *c == b'_')
    };
    let mut arms = Vec::new();
    for (at, _) in code.match_indices("match") {
        if (at > 0 && ident(at - 1)) || ident(at + 5) {
            continue;
        }
        let Some(open) = block_open(b, at + 5) else {
            continue;
        };
        let mut i = open + 1;
        while let Some(arrow) = code[i..].find("=>").map(|j| i + j) {
            if unbalanced(&b[i..arrow]) {
                break;
            }
            let (end, next) = arm_end(b, arrow + 2);
            arms.push((
                arrow,
                code[i..arrow].trim().to_string(),
                code[arrow + 2..end].trim().to_string(),
            ));
            i = next;
            if code[i..].trim_start().starts_with('}') {
                break;
            }
        }
    }
    arms.sort();
    arms.dedup();
    arms
}

/// Whether an arm pattern (its guard dropped) matches anything: `_`, `Some(_)`, `Err(_)`, `(_, _)`.
fn is_wildcard(pattern: &str) -> bool {
    let flat = pattern.split_whitespace().collect::<Vec<_>>().join(" ");
    let p = flat.split(" if ").next().unwrap_or("").trim();
    let p = p.strip_prefix('|').unwrap_or(p).trim_start();
    let p = ["Err", "Ok", "Some"]
        .iter()
        .find_map(|w| p.strip_prefix(w))
        .unwrap_or(p);
    p.contains('_')
        && p.chars()
            .all(|c| matches!(c, '_' | '(' | ')' | ',' | '.' | '|' | ' '))
}

/// Whether an arm body only answers NULL, nothing or a default.
fn is_null_body(body: &str) -> bool {
    let s: String = body.chars().filter(|c| !c.is_whitespace()).collect();
    let s = s
        .strip_prefix('{')
        .and_then(|x| x.strip_suffix('}'))
        .unwrap_or(&s);
    let s = s.strip_suffix(';').unwrap_or(s);
    let s = s
        .strip_prefix("Ok(")
        .and_then(|x| x.strip_suffix(')'))
        .unwrap_or(s);
    let path = |p: &str| {
        !p.is_empty()
            && p.chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | ':' | '.'))
    };
    matches!(s, "None" | "Some(None)" | "default()")
        || s.strip_suffix(".append_null()").is_some_and(path)
        || s.strip_suffix("::Null").is_some_and(path)
        || s.strip_suffix("::default()").is_some_and(path)
}

/// Every wildcard arm answering NULL or a default in the product code under `src/source/`.
fn wildcard_null_arms() -> Vec<String> {
    number(
        product_sources("src/source")
            .into_iter()
            .flat_map(|(file, code)| {
                match_arms(&code)
                    .into_iter()
                    .filter(|(_, p, b)| is_wildcard(p) && is_null_body(b))
                    .map(|(at, p, b)| {
                        format!(
                            "{}::{}: {} => {}",
                            rel(&file),
                            enclosing_fn(&code, at),
                            squash(&p),
                            squash(&b)
                        )
                    })
                    .collect::<Vec<_>>()
            })
            .collect(),
    )
}

/// Whether a field named `name` holds a credential: it ends in a credential word, and is not a secret-manager name such as `url_secret`.
fn credential_name(name: &str) -> bool {
    let words: Vec<&str> = name.split('_').collect();
    let cred = |w: &str| CREDENTIAL_WORDS.contains(&w);
    let last = words.last().copied().unwrap_or("");
    let reference = last == "secret" && words.len() > 1 && cred(words[words.len() - 2]);
    cred(last) && !reference
}

/// What sits outside nested braces and attributes in a struct body.
fn top_level(body: &str) -> String {
    let mut out = String::new();
    let mut braces = 0usize;
    let mut chars = body.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '#' && chars.peek() == Some(&'[') {
            chars.next();
            let mut depth = 1;
            while depth > 0 {
                match chars.next() {
                    Some('[') => depth += 1,
                    Some(']') => depth -= 1,
                    None => break,
                    _ => {}
                }
            }
            continue;
        }
        match c {
            '{' => braces += 1,
            '}' => braces = braces.saturating_sub(1),
            _ if braces == 0 => out.push(c),
            _ => {}
        }
    }
    out
}

/// The `name: Type` fields of one struct body, the type with its blanks removed.
fn fields_of(body: &str) -> Vec<(String, String)> {
    let flat = top_level(body);
    let mut items = Vec::new();
    let (mut depth, mut start) = (0i32, 0);
    let b = flat.as_bytes();
    for (i, c) in b.iter().enumerate() {
        match c {
            b'(' | b'[' | b'<' => depth += 1,
            b')' | b']' => depth -= 1,
            b'>' if i == 0 || b[i - 1] != b'-' => depth -= 1,
            b',' if depth == 0 => {
                items.push(&flat[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    items.push(&flat[start..]);
    items
        .into_iter()
        .filter_map(|item| {
            let item = item.trim();
            let item = match item.strip_prefix("pub") {
                Some(rest) if rest.starts_with('(') => rest.split_once(')')?.1,
                Some(rest) if rest.starts_with(char::is_whitespace) => rest,
                _ => item,
            };
            let (name, ty) = item.split_once(':')?;
            let name = name.trim();
            (!name.is_empty() && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'))
                .then(|| (name.to_string(), ty.split_whitespace().collect()))
        })
        .collect()
}

/// What a `{` opens, read from the code before it.
#[derive(Clone, Copy, PartialEq)]
enum Body {
    Fields,
    Enum,
    Other,
}

/// Every named field of every struct and enum struct-variant in `code`, as (name, type).
fn named_fields(code: &str) -> Vec<(String, String)> {
    let mut open: Vec<(Body, usize)> = Vec::new();
    let mut out = Vec::new();
    for (i, c) in code.char_indices() {
        match c {
            '{' => {
                let before = code[..i].trim_end();
                let head = &before[before.rfind([';', '{', '}']).map_or(0, |p| p + 1)..];
                let words: Vec<&str> = head
                    .split(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
                    .collect();
                let in_enum = open.last().is_some_and(|(k, _)| *k == Body::Enum);
                let last = before
                    .rsplit(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
                    .next()
                    .unwrap_or("");
                let kind = if words.contains(&"struct") {
                    Body::Fields
                } else if words.contains(&"enum") {
                    Body::Enum
                } else if in_enum && last.starts_with(|c: char| c.is_ascii_uppercase()) {
                    Body::Fields
                } else {
                    Body::Other
                };
                open.push((kind, i + 1));
            }
            '}' => {
                if let Some((Body::Fields, start)) = open.pop() {
                    out.extend(fields_of(&code[start..i]));
                }
            }
            _ => {}
        }
    }
    out
}

/// Every credential-named field typed `String` or `Option<String>` in `src/config/`'s product code.
fn credential_strings() -> Vec<String> {
    number(
        product_sources("src/config")
            .into_iter()
            .flat_map(|(file, code)| {
                named_fields(&code)
                    .into_iter()
                    .filter(|(n, t)| {
                        credential_name(n) && matches!(t.as_str(), "String" | "Option<String>")
                    })
                    .map(move |(n, t)| format!("{}: {n}: {t}", rel(&file)))
                    .collect::<Vec<_>>()
            })
            .collect(),
    )
}

/// The offset of every text needle in `code`: a blanked literal searched for (`contains("…")`), compared to (`== "…"`), or listed in an all-literal array.
fn needle_offsets(code: &str) -> Vec<usize> {
    let mut at: Vec<usize> = code
        .match_indices("\"\"")
        .filter(|(at, _)| {
            let before = code[..*at].trim_end();
            before.ends_with("contains(") || before.ends_with("==")
        })
        .map(|(at, _)| at)
        .collect();
    for (open, _) in code.match_indices('[') {
        let rest = &code[open + 1..];
        let Some(end) = rest.find([']', '[']) else {
            continue;
        };
        if rest.as_bytes()[end] != b']' {
            continue;
        }
        let items: Vec<&str> = rest[..end]
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .collect();
        if !items.is_empty() && items.iter().all(|s| *s == "\"\"") {
            at.extend(std::iter::repeat_n(open, items.len()));
        }
    }
    at.sort();
    at
}

/// The text needles of the product code of the crate-relative `file`, one id per needle: its function and its number there.
fn needles_in(file: &str) -> Vec<String> {
    let found = product_sources("src")
        .into_iter()
        .find(|(f, _)| rel(f) == file)
        .unwrap_or_else(|| {
            panic!("{file} is not a product file under src/: the ratchet reads the wrong path")
        });
    number(
        needle_offsets(&found.1)
            .into_iter()
            .map(|at| format!("{file}::{}: needle", enclosing_fn(&found.1, at)))
            .collect(),
    )
}

#[test]
fn wildcard_null_arms_are_each_listed() {
    hold(
        "wildcard-null-arms",
        &wildcard_null_arms(),
        "a value no arm reads is refused with a code (`rivet_bail!(codes::SOURCE_OVERRIDE_WIRE_MISMATCH, …)`), never written as NULL (ADR 0040 I2)",
    );
}

#[test]
fn credential_string_fields_are_each_listed() {
    hold_none(
        "credential-string-fields",
        &credential_strings(),
        "a new credential is read through a reference (`*_env`, `*_file`, `*_secret`) or the I1 `Secret` type, never held as a String (ADR 0040 I1)",
    );
}

#[test]
fn redaction_needles_are_each_listed() {
    hold(
        "redaction-text-needles",
        &needles_in("src/redact.rs"),
        "a new secret shape is masked by its type at the sink, not by one more pattern in the text scanner (ADR 0040 D5)",
    );
}

#[test]
fn retry_text_needles_are_each_listed() {
    hold(
        "retry-text-needles",
        &needles_in("src/error/retry.rs"),
        "a new retry or exit verdict is decided by the error's type or code before its text, not by one more `msg.contains` (ADR 0040 I4)",
    );
}

#[test]
fn the_arm_reader_sees_wildcard_null_arms_and_their_near_misses() {
    let code = "fn f(x: Option<u8>) -> Option<u8> { match x { Some(1) => Some(1), Some(_) if g() => None, _ => None } }\n\
                fn h(c: Cell, b: &mut B) { match c { C::A(v) => b.append_value(v), C::B => { b.append_null() } _ => b.append_null(), } }\n\
                fn k(r: R) -> Result<Option<u8>> { match r { Ok(v) => Ok(Some(v)), Err(_) => Ok(None) } }\n\
                fn m(v: V) -> E { match (v, 1) { (_, _) => E::Null } }\n\
                fn n(v: V) -> u8 { match v { V::A => 1, _ => Default::default() } }\n\
                fn refused(v: V) -> Result<u8> { match v { V::A => Ok(1), _ => rivet_bail!(C, \"\") } }\n\
                fn named(v: V) -> Option<u8> { match v { other_value => None, V::B => Some(2) } }\n\
                fn nested(v: V) -> Option<u8> { match v { V::A => match w { _ => Some(3) }, _ => None } }";
    let hits: Vec<(String, String)> = match_arms(code)
        .into_iter()
        .filter(|(_, p, b)| is_wildcard(p) && is_null_body(b))
        .map(|(_, p, b)| (p, b))
        .collect();
    assert_eq!(
        hits,
        [
            ("Some(_) if g()", "None"),
            ("_", "None"),
            ("_", "b.append_null()"),
            ("Err(_)", "Ok(None)"),
            ("(_, _)", "E::Null"),
            ("_", "Default::default()"),
            ("_", "None"),
        ]
        .map(|(p, b)| (p.to_string(), b.to_string()))
    );
}

#[test]
fn the_field_reader_sees_credential_strings_and_their_near_misses() {
    let code = "pub struct S { pub url: Option<String>, pub url_secret: Option<String>, pub(crate) password: String,\n\
                #[serde(default)] password_env: Option<String>, client_secret: String, secret_key: String,\n\
                account_key: Option<String>, key_column: String, private_key_path: Option<String>, token: u32,\n\
                hook: Box<dyn Fn(u8) -> String>, webhook_url: Option<String> }\n\
                pub enum L { Clickhouse { url: String, database: String }, Other(String), Bq { api_key: Option<String> } }\n\
                fn f(url: String) { let password: String = x(); }\n\
                impl S { fn g(&self) -> Option<String> { None } }";
    let found: Vec<String> = named_fields(code)
        .into_iter()
        .filter(|(n, t)| credential_name(n) && matches!(t.as_str(), "String" | "Option<String>"))
        .map(|(n, _)| n)
        .collect();
    assert_eq!(
        found,
        [
            "url",
            "password",
            "client_secret",
            "secret_key",
            "account_key",
            "webhook_url",
            "url",
            "api_key"
        ]
    );
}

#[test]
fn the_needle_reader_counts_searched_compared_and_listed_literals_only() {
    let code = "fn f(msg: &str) -> bool { msg.contains(\"\") || msg.contains(\"\") || k == \"\" || [\"\", \"\"].iter().any(|w| msg.ends_with(w)) }\n\
                fn g() -> String { format!(\"\", x) + \"\" }\n\
                const T: [&str; 2] = [\"\", \"\",];\n\
                fn h(c: &str) -> bool { c.starts_with(\"\") || [a, \"\"].contains(&c) }";
    assert_eq!(needle_offsets(code).len(), 7);
}
