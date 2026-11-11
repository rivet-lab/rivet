//! A count of code sites is a file of lines, one per site, never a number: a new site needs its own line, a gone site takes its line with it.

use std::collections::BTreeSet;

/// The ledger directory, relative to the crate root.
const DIR: &str = "tests/offline/ledgers";

/// `s` with every run of whitespace folded to one space.
pub(crate) fn squash(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// The name of the last `fn` opened before offset `at` in `code`, `-` when there is none.
pub(crate) fn enclosing_fn(code: &str, at: usize) -> String {
    let head = &code[..at.min(code.len())];
    head.rmatch_indices("fn ")
        .find(|(i, _)| !head[..*i].ends_with(|c: char| c.is_ascii_alphanumeric() || c == '_'))
        .map(|(i, _)| {
            head[i + 3..]
                .chars()
                .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
                .collect()
        })
        .filter(|n: &String| !n.is_empty())
        .unwrap_or_else(|| "-".to_string())
}

/// `ids` with the second and later copy of an id suffixed ` #2`, ` #3`, so every site has its own line.
pub(crate) fn number(ids: Vec<String>) -> Vec<String> {
    let mut seen: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    ids.into_iter()
        .map(|id| {
            let n = seen.entry(id.clone()).or_insert(0);
            *n += 1;
            if *n == 1 { id } else { format!("{id} #{n}") }
        })
        .collect()
}

/// The `(id, reason)` of every ledger line; a line without a tab, a date and a reason, or a repeated id, is an error.
fn parse(text: &str) -> Result<Vec<(String, String)>, String> {
    let mut out: Vec<(String, String)> = Vec::new();
    for (i, line) in text
        .lines()
        .enumerate()
        .filter(|(_, l)| !l.trim().is_empty())
    {
        let Some((id, reason)) = line.split_once('\t') else {
            return Err(format!(
                "line {}: `{line}` is not `<site id><TAB><YYYY-MM-DD reason>`",
                i + 1
            ));
        };
        let (date, why) = reason.split_once(' ').unwrap_or((reason, ""));
        let b = date.as_bytes();
        let dated = b.len() == 10
            && b.iter().enumerate().all(|(k, c)| {
                if k == 4 || k == 7 {
                    *c == b'-'
                } else {
                    c.is_ascii_digit()
                }
            });
        if id.is_empty() || !dated || why.trim().len() < 8 {
            return Err(format!(
                "line {}: `{id}` needs a date YYYY-MM-DD and a reason of a few words",
                i + 1
            ));
        }
        if out.iter().any(|(seen, _)| seen == id) {
            return Err(format!("line {}: `{id}` is listed twice", i + 1));
        }
        out.push((id.to_string(), reason.to_string()));
    }
    Ok(out)
}

/// Whether `sites` and the ledger `text` name the same sites; the error lists each unlisted site and each stale line.
pub(crate) fn compare(sites: &[String], text: &str) -> Result<(), String> {
    let listed: BTreeSet<String> = parse(text)?.into_iter().map(|(id, _)| id).collect();
    let found: BTreeSet<String> = sites.iter().cloned().collect();
    if found.len() != sites.len() {
        return Err("the scanner named one site twice: its ids must be unique".to_string());
    }
    let new: Vec<_> = found.difference(&listed).collect();
    let gone: Vec<_> = listed.difference(&found).collect();
    if new.is_empty() && gone.is_empty() {
        return Ok(());
    }
    let mut msg = String::new();
    if !new.is_empty() {
        msg += &format!(
            "{} site(s) with no ledger line (add `<id><TAB><today> <why it stays, what removes it>`):\n",
            new.len()
        );
        msg += &new
            .iter()
            .map(|s| format!("  {s}"))
            .collect::<Vec<_>>()
            .join("\n");
        msg += "\n";
    }
    if !gone.is_empty() {
        msg += &format!(
            "{} ledger line(s) whose site is gone (delete the line):\n",
            gone.len()
        );
        msg += &gone
            .iter()
            .map(|s| format!("  {s}"))
            .collect::<Vec<_>>()
            .join("\n");
        msg += "\n";
    }
    Err(msg)
}

/// Fails unless the sites found equal the lines of `tests/offline/ledgers/<ledger>.txt`.
pub(crate) fn hold(ledger: &str, sites: &[String], remedy: &str) {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join(DIR)
        .join(format!("{ledger}.txt"));
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    if let Err(msg) = compare(sites, &text) {
        panic!("{ledger} ({}): {remedy}\n{msg}", path.display());
    }
    assert!(
        !sites.is_empty(),
        "{ledger}: the scanner found no site, so it is blind and a removal would read as green"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    const LEDGER: &str = "a.rs::f: x\t2026-11-11 goes with the I2 table\nb.rs::g: y\t2026-11-11 goes with the I2 table\n";

    fn ids(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn a_site_with_no_line_and_a_line_with_no_site_each_fail() {
        assert!(compare(&ids(&["a.rs::f: x", "b.rs::g: y"]), LEDGER).is_ok());
        let new = compare(&ids(&["a.rs::f: x", "b.rs::g: y", "c.rs::h: z"]), LEDGER).unwrap_err();
        assert!(
            new.contains("no ledger line") && new.contains("c.rs::h: z"),
            "{new}"
        );
        let gone = compare(&ids(&["a.rs::f: x"]), LEDGER).unwrap_err();
        assert!(
            gone.contains("site is gone") && gone.contains("b.rs::g: y"),
            "{gone}"
        );
    }

    #[test]
    fn a_line_needs_its_date_and_its_reason_and_an_id_once() {
        for bad in [
            "a\tno date here at all",
            "a\t2026-11-11",
            "a\t2026-1-11 short date reason",
            "no tab here",
            "a\t2026-11-11 fine reason\na\t2026-11-11 fine again",
        ] {
            assert!(parse(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn repeated_ids_are_numbered_and_the_function_is_read_from_the_text() {
        assert_eq!(
            number(ids(&["a", "b", "a", "a"])),
            ids(&["a", "b", "a #2", "a #3"])
        );
        let code = "fn outer() { x } pub(crate) fn inner(a: u8) { match a { _ => None } }";
        assert_eq!(enclosing_fn(code, code.find("match").unwrap()), "inner");
        assert_eq!(enclosing_fn("const X: u8 = 1;", 5), "-");
        assert_eq!(enclosing_fn("let defn = 1; foo", 17), "-");
    }
}
