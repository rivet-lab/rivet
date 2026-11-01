//! Shared seams of `src/`: a seam's bypass shapes appear only in its owner file and at the listed exceptions; that list only shrinks.

use std::collections::{BTreeMap, BTreeSet};

use serde::Deserialize;

use super::module_layer_guard::{
    Tok, Toks, closer_of, ident_at, lex, mod_declaration, skip_item, src_files, test_gate, tok,
};

const SEAMS_PATH: &str = "docs/seams.yaml";
const LEDGER_PATH: &str = "docs/roast-ledger.yaml";

/// Keys every seam entry carries, in the order `docs/seams.yaml` lists them in `schema`.
const SEAM_KEYS: &[&str] = &[
    "id",
    "owner",
    "reads",
    "scope",
    "also_in",
    "bypass",
    "exceptions",
];

/// Keys every roast-ledger entry carries, in the order `docs/roast-ledger.yaml` lists them in `schema`.
const LEDGER_KEYS: &[&str] = &[
    "id", "date", "lens", "title", "copies", "status", "verdict", "closes",
];

/// The statuses a roast-ledger entry may hold.
const LEDGER_STATUSES: &[&str] = &[
    "accepted-as-bugfix",
    "deferred-past-gate",
    "rejected",
    "done",
];

/// Seam bypass exceptions listed in docs/seams.yaml. Shrink-only.
// 0 -> 34 (2026-10-31): first registry; 8 manifest-census, 9 run-set, 11 url-authority, 1 object-store-retry, 5 cdc-captured-tables; each closes with the fix named on its entry.
// 34 -> 23 (2026-11-01): the 11 url-authority exceptions closed by routing every reader through config::url.
const SEAM_EXCEPTION_CEILING: usize = 23; // ratchet-pin: seam-bypass-exceptions

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Registry {
    schema: Vec<String>,
    seams: Vec<Seam>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Seam {
    id: String,
    owner: String,
    reads: String,
    #[serde(default)]
    scope: Vec<String>,
    #[serde(default)]
    also_in: Vec<String>,
    bypass: Vec<String>,
    exceptions: Vec<Exception>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Exception {
    at: String,
    dated: String,
    reason: String,
    closes: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Ledger {
    schema: Vec<String>,
    statuses: Vec<String>,
    entries: Vec<serde_yaml_ng::Mapping>,
}

/// A product source file: its repo-relative path and its tokens outside `#[cfg(test)]` items.
pub(crate) struct Product {
    pub(crate) rel: String,
    pub(crate) toks: Toks,
}

/// The tokens of `t` without `#[cfg(test)]` / `#[test]` items, and the names of the out-of-line test modules it declares.
fn without_test_items(t: &Toks) -> (Toks, Vec<String>) {
    let (mut out, mut test_mods, mut i) = (Toks::new(), Vec::new(), 0);
    while i < t.len() {
        if t[i].0 == Tok::Punct('#') {
            let inner = tok(t, i + 1) == Some(&Tok::Punct('!'));
            let open = i + 1 + usize::from(inner);
            if tok(t, open) == Some(&Tok::Punct('[')) {
                let close = closer_of(t, open).min(t.len());
                let attr = &t[(open + 1).min(close)..close];
                let bare_test = attr.len() == 1 && ident_at(attr, 0) == Some("test");
                if bare_test || test_gate(attr) == Some(true) {
                    if inner {
                        break;
                    }
                    test_mods.extend(mod_declaration(t, close + 1));
                    i = skip_item(t, close + 1);
                    continue;
                }
            }
        }
        out.push(t[i].clone());
        i += 1;
    }
    (out, test_mods)
}

/// Whether `rel` is `name`'s own file or sits under it, for a module declared in `rel_of_parent`.
fn is_test_module_file(rel: &str, parent: &str, name: &str) -> bool {
    let dir = match parent.rsplit_once('/') {
        Some((d, "mod.rs" | "lib.rs" | "main.rs")) => d.to_string(),
        Some((d, f)) => format!("{d}/{}", f.trim_end_matches(".rs")),
        None => String::new(),
    };
    let base = format!("{dir}/{name}");
    rel == format!("{base}.rs") || rel.starts_with(&format!("{base}/"))
}

/// Every product file of `raw` (repo-relative path, text): test modules and test-only files dropped.
pub(crate) fn product_files(raw: &[(String, String)]) -> Vec<Product> {
    let lexed: Vec<(String, Toks, Vec<String>)> = raw
        .iter()
        .map(|(rel, text)| {
            let (toks, mods) = without_test_items(&lex(text));
            (rel.clone(), toks, mods)
        })
        .collect();
    let declared: Vec<(&str, &str)> = lexed
        .iter()
        .flat_map(|(rel, _, mods)| mods.iter().map(move |m| (rel.as_str(), m.as_str())))
        .collect();
    let test_only: BTreeSet<String> = lexed
        .iter()
        .filter(|(rel, ..)| {
            declared
                .iter()
                .any(|(parent, name)| is_test_module_file(rel, parent, name))
        })
        .map(|(rel, ..)| rel.clone())
        .collect();
    lexed
        .into_iter()
        .filter(|(rel, ..)| !test_only.contains(rel))
        .map(|(rel, toks, _)| Product { rel, toks })
        .collect()
}

/// The product files of `src/`, with `src/`-prefixed repo-relative paths.
pub(crate) fn src_product() -> Vec<Product> {
    let raw: Vec<(String, String)> = src_files()
        .into_iter()
        .map(|(rel, text)| (format!("src/{rel}"), text))
        .collect();
    product_files(&raw)
}

/// (name, body open index, body close index) of every `fn` with a body, outermost and nested.
pub(crate) fn fn_spans(t: &Toks) -> Vec<(String, usize, usize)> {
    let mut spans = Vec::new();
    for i in 0..t.len() {
        let (Some("fn"), Some(name)) = (ident_at(t, i), ident_at(t, i + 1)) else {
            continue;
        };
        let mut j = i + 2;
        while j < t.len() {
            match tok(t, j) {
                Some(Tok::Punct('(' | '[')) => {
                    j = closer_of(t, j) + 1;
                    continue;
                }
                Some(Tok::Punct('{')) => {
                    spans.push((name.to_string(), j, closer_of(t, j)));
                    break;
                }
                Some(Tok::Punct(';')) => break,
                _ => {}
            }
            j += 1;
        }
    }
    spans
}

/// The innermost `fn` whose body holds token `at`, or `<module>`.
fn enclosing_fn(spans: &[(String, usize, usize)], at: usize) -> &str {
    spans
        .iter()
        .filter(|(_, open, close)| *open < at && at < *close)
        .max_by_key(|(_, open, _)| *open)
        .map_or("<module>", |(name, ..)| name.as_str())
}

/// The token shape of a pattern written as Rust text.
fn pattern_tokens(pattern: &str) -> Vec<Tok> {
    lex(pattern).into_iter().map(|(t, _)| t).collect()
}

/// `file::fn` of every place `seam`'s patterns match in `files` outside the owner: a definition (`fn name`) never matches.
fn found_bypasses(seam: &Seam, files: &[Product]) -> BTreeMap<String, usize> {
    let owner = seam.owner.split_once("::").map_or("", |(p, _)| p);
    let scope: Vec<&str> = if seam.scope.is_empty() {
        vec!["src/"]
    } else {
        seam.scope.iter().map(String::as_str).collect()
    };
    let patterns: Vec<Vec<Tok>> = seam.bypass.iter().map(|p| pattern_tokens(p)).collect();
    let mut found = BTreeMap::new();
    for f in files {
        let home = f.rel == owner || seam.also_in.contains(&f.rel);
        if home || !scope.iter().any(|s| f.rel.starts_with(s)) {
            continue;
        }
        let spans = fn_spans(&f.toks);
        for pat in patterns.iter().filter(|p| !p.is_empty()) {
            for at in 0..f.toks.len() {
                let hit = pat
                    .iter()
                    .enumerate()
                    .all(|(k, want)| f.toks.get(at + k).is_some_and(|(have, _)| have == want));
                let defined = at > 0 && ident_at(&f.toks, at - 1) == Some("fn");
                if hit && !defined {
                    let key = format!("{}::{}", f.rel, enclosing_fn(&spans, at));
                    found.entry(key).or_insert(f.toks[at].1);
                }
            }
        }
    }
    found
}

/// Whether `item` is declared as a fn, type, constant or module in `toks`.
fn declares(toks: &Toks, item: &str) -> bool {
    (1..toks.len()).any(|i| {
        ident_at(toks, i) == Some(item)
            && matches!(
                ident_at(toks, i - 1),
                Some("fn" | "struct" | "enum" | "trait" | "const" | "static" | "type" | "mod")
            )
    })
}

/// `YYYY-MM-DD`.
fn is_date(s: &str) -> bool {
    let b = s.as_bytes();
    b.len() == 10
        && b.iter().enumerate().all(|(i, c)| {
            if matches!(i, 4 | 7) {
                *c == b'-'
            } else {
                c.is_ascii_digit()
            }
        })
}

/// Every way the registry and the tree disagree, one line each.
fn seam_verdict(reg: &Registry, files: &[Product]) -> Vec<String> {
    let mut errors = Vec::new();
    let keys: Vec<&str> = reg.schema.iter().map(String::as_str).collect();
    if keys != SEAM_KEYS {
        errors.push(format!("schema {keys:?} is not {SEAM_KEYS:?}"));
    }
    let mut ids = BTreeSet::new();
    for seam in &reg.seams {
        let id = &seam.id;
        if !ids.insert(id.clone()) {
            errors.push(format!("seam {id}: id declared twice"));
        }
        if seam.bypass.is_empty() || seam.reads.trim().is_empty() {
            errors.push(format!("seam {id}: no bypass pattern or no `reads`"));
        }
        for p in &seam.bypass {
            if pattern_tokens(p).is_empty() {
                errors.push(format!("seam {id}: bypass pattern {p:?} lexes to nothing"));
            }
        }
        let (path, item) = seam.owner.split_once("::").unwrap_or((&seam.owner, ""));
        match files.iter().find(|f| f.rel == path) {
            Some(f) if !item.is_empty() && declares(&f.toks, item) => {}
            Some(_) => errors.push(format!(
                "seam {id}: owner {} is not declared in {path}",
                seam.owner
            )),
            None => errors.push(format!(
                "seam {id}: owner file {path} is not a product source"
            )),
        }
        for extra in &seam.also_in {
            if !files.iter().any(|f| &f.rel == extra) {
                errors.push(format!(
                    "seam {id}: also_in {extra} is not a product source"
                ));
            }
        }
        let found = found_bypasses(seam, files);
        let listed: BTreeSet<&str> = seam.exceptions.iter().map(|e| e.at.as_str()).collect();
        for (at, line) in &found {
            if !listed.contains(at.as_str()) {
                errors.push(format!(
                    "seam {id}: bypass at {at} (line {line}) is not an exception: route it through {} or list it",
                    seam.owner
                ));
            }
        }
        for e in &seam.exceptions {
            if !found.contains_key(&e.at) {
                errors.push(format!(
                    "seam {id}: stale exception {}: no bypass matches it any more",
                    e.at
                ));
            }
            let blank = e.reason.trim().is_empty() || e.closes.trim().is_empty();
            if blank || !is_date(&e.dated) {
                errors.push(format!(
                    "seam {id}: exception {} needs a YYYY-MM-DD date, a reason and what closes it",
                    e.at
                ));
            }
        }
        let mut seen = BTreeSet::new();
        for e in &seam.exceptions {
            if !seen.insert(&e.at) {
                errors.push(format!("seam {id}: exception {} listed twice", e.at));
            }
        }
    }
    errors
}

/// The registry parsed from `text`.
fn parse_registry(text: &str) -> Registry {
    serde_yaml_ng::from_str(text).unwrap_or_else(|e| panic!("{SEAMS_PATH} does not parse: {e}"))
}

/// Every way the roast ledger breaks its schema, one line each.
fn ledger_verdict(text: &str) -> Vec<String> {
    let ledger: Ledger = match serde_yaml_ng::from_str(text) {
        Ok(l) => l,
        Err(e) => return vec![format!("{LEDGER_PATH} does not parse: {e}")],
    };
    let mut errors = Vec::new();
    let keys: Vec<&str> = ledger.schema.iter().map(String::as_str).collect();
    if keys != LEDGER_KEYS {
        errors.push(format!("schema {keys:?} is not {LEDGER_KEYS:?}"));
    }
    let statuses: Vec<&str> = ledger.statuses.iter().map(String::as_str).collect();
    if statuses != LEDGER_STATUSES {
        errors.push(format!("statuses {statuses:?} is not {LEDGER_STATUSES:?}"));
    }
    let mut ids = BTreeSet::new();
    for (n, entry) in ledger.entries.iter().enumerate() {
        let text_of = |k: &str| entry.get(k).and_then(|v| v.as_str()).map(str::trim);
        let id = text_of("id").unwrap_or("?");
        let here = format!("entry {n} ({id})");
        let present: BTreeSet<&str> = entry.keys().filter_map(|k| k.as_str()).collect();
        for k in LEDGER_KEYS {
            if !present.contains(k) {
                errors.push(format!("{here}: missing key {k}"));
            }
        }
        for k in &present {
            if !LEDGER_KEYS.contains(k) {
                errors.push(format!("{here}: unknown key {k}"));
            }
        }
        for k in LEDGER_KEYS.iter().filter(|k| **k != "copies") {
            if text_of(k).is_none_or(str::is_empty) {
                errors.push(format!("{here}: key {k} is not a non-empty string"));
            }
        }
        if !entry
            .get("copies")
            .and_then(|c| c.as_sequence())
            .is_some_and(|c| {
                !c.is_empty()
                    && c.iter()
                        .all(|x| x.as_str().is_some_and(|s| !s.trim().is_empty()))
            })
        {
            errors.push(format!("{here}: copies is not a non-empty list of strings"));
        }
        if text_of("status").is_some_and(|s| !LEDGER_STATUSES.contains(&s)) {
            errors.push(format!("{here}: unknown status {:?}", text_of("status")));
        }
        if text_of("date").is_some_and(|d| !is_date(d)) {
            errors.push(format!("{here}: date is not YYYY-MM-DD"));
        }
        if !ids.insert(id.to_string()) {
            errors.push(format!("{here}: id declared twice"));
        }
    }
    if ledger.entries.is_empty() {
        errors.push("the ledger holds no entry".to_string());
    }
    errors
}

/// A one-seam registry over in-memory files, for the guard's own tests.
fn registry(exceptions: &[&str], bypass: &str) -> Registry {
    let ex: String = exceptions
        .iter()
        .map(|at| format!("      - {{at: {at}, dated: 2026-10-31, reason: r, closes: c}}\n"))
        .collect();
    let ex = if ex.is_empty() {
        "    exceptions: []\n".to_string()
    } else {
        format!("    exceptions:\n{ex}")
    };
    parse_registry(&format!(
        "schema: [{}]\nseams:\n  - id: s\n    owner: src/own.rs::own\n    reads: x\n    bypass: [{bypass:?}]\n{ex}",
        SEAM_KEYS.join(", ")
    ))
}

/// Product files from in-memory (path, text) pairs.
fn tree(files: &[(&str, &str)]) -> Vec<Product> {
    let raw: Vec<(String, String)> = files
        .iter()
        .map(|(r, t)| (r.to_string(), t.to_string()))
        .collect();
    product_files(&raw)
}

const OWNER: (&str, &str) = ("src/own.rs", "pub fn own() { read_it(1); }");

#[test]
fn the_registry_matches_the_tree() {
    let text = super::nonvacuity::subject_text(SEAMS_PATH);
    let reg = parse_registry(&text);
    super::nonvacuity::require_enumerated(
        reg.seams.len(),
        5,
        "seams",
        "docs/seams.yaml lost entries",
    );
    let errors = seam_verdict(&reg, &src_product());
    assert_eq!(
        errors,
        Vec::<String>::new(),
        "{SEAMS_PATH} and src/ disagree"
    );
}

#[test]
fn exceptions_only_shrink() {
    let reg = parse_registry(&super::nonvacuity::subject_text(SEAMS_PATH));
    let n: usize = reg.seams.iter().map(|s| s.exceptions.len()).sum();
    assert!(
        n <= SEAM_EXCEPTION_CEILING,
        "{n} seam exceptions, ceiling {SEAM_EXCEPTION_CEILING}: route the new bypass through its owner"
    );
}

#[test]
fn the_roast_ledger_holds_its_schema() {
    let text = super::nonvacuity::subject_text(LEDGER_PATH);
    let errors = ledger_verdict(&text);
    assert_eq!(errors, Vec::<String>::new(), "{LEDGER_PATH}");
}

#[test]
fn a_ledger_entry_missing_a_key_or_holding_an_unknown_status_fails() {
    let head = format!(
        "schema: [{}]\nstatuses: [{}]\nentries:\n",
        LEDGER_KEYS.join(", "),
        LEDGER_STATUSES.join(", ")
    );
    let full = "  - {id: a, date: 2026-10-31, lens: l, title: t, copies: [x.rs:1], status: done, verdict: v, closes: c}\n";
    assert!(ledger_verdict(&format!("{head}{full}")).is_empty());
    let no_closes = "  - {id: a, date: 2026-10-31, lens: l, title: t, copies: [x.rs:1], status: done, verdict: v}\n";
    assert!(
        ledger_verdict(&format!("{head}{no_closes}"))
            .iter()
            .any(|e| e.contains("missing key closes"))
    );
    let odd = full.replace("done", "maybe");
    assert!(
        ledger_verdict(&format!("{head}{odd}"))
            .iter()
            .any(|e| e.contains("unknown status"))
    );
    let extra = full.replace("}\n", ", note: n}\n");
    assert!(
        ledger_verdict(&format!("{head}{extra}"))
            .iter()
            .any(|e| e.contains("unknown key note"))
    );
    assert!(
        ledger_verdict(&format!("{head}{full}{full}"))
            .iter()
            .any(|e| e.contains("declared twice"))
    );
    assert!(!ledger_verdict(&head).is_empty());
}

#[test]
fn a_new_bypass_outside_the_exceptions_fails() {
    let files = tree(&[OWNER, ("src/other.rs", "fn caller() { read_it ( 2 ); }")]);
    let errors = seam_verdict(&registry(&[], "read_it ("), &files);
    assert!(
        errors
            .iter()
            .any(|e| e.contains("bypass at src/other.rs::caller")),
        "{errors:?}"
    );
}

#[test]
fn an_exception_whose_bypass_is_gone_is_stale() {
    let files = tree(&[OWNER, ("src/other.rs", "fn caller() { fine(); }")]);
    let errors = seam_verdict(&registry(&["src/other.rs::caller"], "read_it ("), &files);
    assert!(
        errors
            .iter()
            .any(|e| e.contains("stale exception src/other.rs::caller")),
        "{errors:?}"
    );
}

#[test]
fn an_owner_that_no_longer_exists_fails() {
    let files = tree(&[("src/own.rs", "pub fn renamed() {}")]);
    let errors = seam_verdict(&registry(&[], "read_it ("), &files);
    assert!(
        errors
            .iter()
            .any(|e| e.contains("owner src/own.rs::own is not declared")),
        "{errors:?}"
    );
    let errors = seam_verdict(&registry(&[], "read_it ("), &tree(&[]));
    assert!(
        errors
            .iter()
            .any(|e| e.contains("owner file src/own.rs is not a product source")),
        "{errors:?}"
    );
}

#[test]
fn the_owner_a_definition_a_comment_a_string_and_test_code_are_not_bypasses() {
    let other = "fn read_it() {}\n// read_it ( 1 )\nconst S: &str = \"read_it ( 1 )\";\n#[cfg(test)]\nmod tests { fn t() { read_it ( 1 ); } }\n#[test]\nfn loose() { read_it ( 1 ); }\n";
    let files = tree(&[
        OWNER,
        ("src/other.rs", other),
        ("src/m.rs", "#[cfg(test)]\nmod tests;"),
        ("src/m/tests.rs", "fn t() { read_it ( 1 ); }"),
    ]);
    assert_eq!(
        seam_verdict(&registry(&[], "read_it ("), &files),
        Vec::<String>::new()
    );
}

#[test]
fn a_pattern_reads_literals_and_whole_token_shapes() {
    let files = tree(&[
        OWNER,
        (
            "src/other.rs",
            "fn a(s: &str) { s.rfind('@'); s.rfind('#'); }",
        ),
    ]);
    let found = found_bypasses(&registry(&[], "rfind ( '@' )").seams[0], &files);
    assert_eq!(found.keys().collect::<Vec<_>>(), ["src/other.rs::a"]);
    let none = found_bypasses(&registry(&[], "rfind ( '!' )").seams[0], &files);
    assert!(none.is_empty());
}
