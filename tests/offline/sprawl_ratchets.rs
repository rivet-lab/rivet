//! Shrink-only ceilings for sprawl: orphan Python modules, blanket dead-code allows, duplicate function bodies.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use regex::Regex;

use super::module_layer_guard::{
    Tok, Toks, closer_of, ident_at, lex, read_rust_files, src_files, tok,
};
use super::seam_bypass_guard::{Product, fn_spans, product_files};

/// Fewest token-bearing lines a function body needs to count as a duplicate.
const DUPLICATE_MIN_LINES: usize = 8;

/// Largest corpus file read when looking for references to a Python module.
const CORPUS_FILE_CAP: u64 = 2_000_000;

/// Directories no scan descends into.
const SKIPPED_DIRS: &[&str] = &[
    ".git",
    ".claude",
    ".venv",
    "venv",
    "target",
    "node_modules",
    "__pycache__",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
];

/// Extensions of the text files that may reference a Python module.
const CORPUS_EXTENSIONS: &[&str] = &[
    "py", "rs", "toml", "yaml", "yml", "md", "sh", "json", "txt", "cfg", "mk", "html", "js",
];

/// Python modules under dev/ that no other file references and that are not entry points.
// 0 (2026-10-31): first measurement on main ff0a4f2c after #744 deleted the last orphans.
const PYTHON_ORPHAN_CEILING: usize = 0; // ratchet-pin: python-orphans

/// Blanket module-level `#![allow(dead_code)]` in src/.
// 0 -> 8 (2026-10-31): first measurement on main ff0a4f2c; the plan/ and CDC reader modules, each closes when its dead items are deleted.
const SRC_BLANKET_ALLOW_CEILING: usize = 8; // ratchet-pin: src-blanket-dead-code-allows

/// Blanket module-level `#![allow(dead_code)]` in tests/common.
// 0 -> 17 (2026-10-31): first measurement on main ff0a4f2c; the helper modules are shared across test binaries that each use a subset.
const COMMON_BLANKET_ALLOW_CEILING: usize = 17; // ratchet-pin: tests-common-blanket-dead-code-allows

/// Groups of functions in different files of src/ with identical bodies of `DUPLICATE_MIN_LINES` lines or more.
// 0 -> 1 (2026-10-31): first measurement on main ff0a4f2c; pipeline/mod.rs::clamp_line and pipeline/summary.rs::clamp_chars, closes when one calls the other.
const DUPLICATE_BODY_CEILING: usize = 1; // ratchet-pin: duplicate-fn-bodies

/// Every text file under `dir` that may hold a reference, as (repo-relative path, text).
fn corpus(root: &Path, dir: &Path, out: &mut Vec<(String, String)>) {
    let Ok(read) = std::fs::read_dir(dir) else {
        return;
    };
    for e in read.flatten() {
        let p = e.path();
        let Ok(ft) = e.file_type() else { continue };
        if ft.is_symlink() {
            continue;
        }
        let name = e.file_name().to_string_lossy().to_string();
        if ft.is_dir() {
            if !SKIPPED_DIRS.contains(&name.as_str()) {
                corpus(root, &p, out);
            }
        } else if ft.is_file() {
            let ext = p.extension().and_then(|x| x.to_str()).unwrap_or("");
            let wanted = CORPUS_EXTENSIONS.contains(&ext) || name == "Makefile";
            let small = e.metadata().is_ok_and(|m| m.len() <= CORPUS_FILE_CAP);
            if let (true, true, Ok(text)) = (wanted, small, std::fs::read_to_string(&p)) {
                let rel = p
                    .strip_prefix(root)
                    .unwrap()
                    .to_string_lossy()
                    .replace('\\', "/");
                out.push((rel, text));
            }
        }
    }
    out.sort();
}

/// The text files of the repository.
fn repo_corpus() -> Vec<(String, String)> {
    let root = super::nonvacuity::repo_root();
    let mut files = Vec::new();
    corpus(&root, &root, &mut files);
    files
}

/// Whether `text` runs as a script.
fn is_entry_point(text: &str) -> bool {
    text.contains("__name__ == \"__main__\"") || text.contains("__name__ == '__main__'")
}

/// Whether `rel` is a module the Python tooling loads by convention.
fn loaded_by_convention(rel: &str) -> bool {
    let file = rel.rsplit('/').next().unwrap_or(rel);
    file == "__init__.py"
        || file == "__main__.py"
        || file == "conftest.py"
        || file.starts_with("test_")
        || file.ends_with("_test.py")
}

/// The modules under dev/ that nothing else references and that no one runs, sorted.
fn python_orphans(files: &[(String, String)]) -> Vec<String> {
    let import_lines: Vec<(&str, String)> = files
        .iter()
        .filter(|(rel, _)| rel.ends_with(".py"))
        .map(|(rel, text)| {
            let lines: Vec<&str> = text
                .lines()
                .filter(|l| {
                    let l = l.trim_start();
                    l.starts_with("import ") || l.starts_with("from ")
                })
                .collect();
            (rel.as_str(), lines.join("\n"))
        })
        .collect();
    let mut orphans = Vec::new();
    for (rel, text) in files {
        if !rel.starts_with("dev/")
            || !rel.ends_with(".py")
            || loaded_by_convention(rel)
            || is_entry_point(text)
        {
            continue;
        }
        let stem = rel.trim_end_matches(".py");
        let tail: Vec<&str> = stem.rsplit('/').take(2).collect();
        let name = tail[0];
        let parent = tail.get(1).copied().unwrap_or("");
        let path_ref = format!("{parent}/{name}.py");
        let dotted_ref = format!("{parent}.{name}");
        let by_import = Regex::new(&format!(r"\b{}\b", regex::escape(name))).unwrap();
        let referenced = files.iter().any(|(other, other_text)| {
            other != rel && (other_text.contains(&path_ref) || other_text.contains(&dotted_ref))
        }) || import_lines
            .iter()
            .any(|(other, lines)| *other != rel && by_import.is_match(lines));
        if !referenced {
            orphans.push(rel.clone());
        }
    }
    orphans
}

/// `file:line` of every inner `#![allow(.. dead_code ..)]` in `files`.
fn blanket_dead_code_allows(files: &[(String, String)]) -> Vec<String> {
    let mut found = Vec::new();
    for (rel, text) in files {
        let t: Toks = lex(text);
        for i in 0..t.len() {
            let inner = t[i].0 == Tok::Punct('#') && tok(&t, i + 1) == Some(&Tok::Punct('!'));
            if !inner || tok(&t, i + 2) != Some(&Tok::Punct('[')) {
                continue;
            }
            let close = closer_of(&t, i + 2).min(t.len());
            let attr = &t[i + 3..close.max(i + 3)];
            let names = |n: &str| (0..attr.len()).any(|k| ident_at(attr, k) == Some(n));
            if names("allow") && names("dead_code") {
                found.push(format!("{rel}:{}", t[i].1));
            }
        }
    }
    found
}

/// Groups of `file::fn` whose bodies are token-identical across at least two files, each at least `min_lines` token-bearing lines.
fn duplicate_bodies(files: &[Product], min_lines: usize) -> Vec<Vec<String>> {
    let mut groups: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for f in files {
        let spans = fn_spans(&f.toks);
        for (name, open, close) in &spans {
            let nested = spans.iter().any(|(_, o, c)| o < open && close < c);
            if nested || *close <= *open + 1 {
                continue;
            }
            let body = &f.toks[open + 1..*close];
            let lines: BTreeSet<usize> = body.iter().map(|(_, l)| *l).collect();
            if lines.len() < min_lines {
                continue;
            }
            let key: String = body
                .iter()
                .map(|(t, _)| format!("{t:?}"))
                .collect::<Vec<_>>()
                .join(" ");
            groups
                .entry(key)
                .or_default()
                .push(format!("{}::{name}", f.rel));
        }
    }
    groups
        .into_values()
        .filter(|g| {
            g.iter()
                .map(|m| m.split("::").next().unwrap_or(""))
                .collect::<BTreeSet<_>>()
                .len()
                > 1
        })
        .map(|mut g| {
            g.sort();
            g
        })
        .collect()
}

/// The product files of src/.
fn src_products() -> Vec<Product> {
    let raw: Vec<(String, String)> = src_files()
        .into_iter()
        .map(|(rel, text)| (format!("src/{rel}"), text))
        .collect();
    product_files(&raw)
}

/// The files of tests/common, with `tests/common/`-prefixed paths.
fn common_files() -> Vec<(String, String)> {
    let root = super::nonvacuity::repo_root().join("tests/common");
    let mut files = Vec::new();
    read_rust_files(&root, &root, &mut files);
    files
        .into_iter()
        .map(|(rel, text)| (format!("tests/common/{rel}"), text))
        .collect()
}

#[test]
#[allow(clippy::absurd_extreme_comparisons)]
fn python_orphans_never_grow() {
    let files = repo_corpus();
    let py = files
        .iter()
        .filter(|(r, _)| r.starts_with("dev/") && r.ends_with(".py"))
        .count();
    super::nonvacuity::require_enumerated(py, 50, "dev/ python modules", "dev/ moved");
    let orphans = python_orphans(&files);
    assert!(
        orphans.len() <= PYTHON_ORPHAN_CEILING,
        "{} orphan python modules, ceiling {PYTHON_ORPHAN_CEILING}: delete them or wire a caller:\n{}",
        orphans.len(),
        orphans.join("\n")
    );
}

#[test]
fn blanket_dead_code_allows_in_src_never_grow() {
    let raw: Vec<(String, String)> = src_files()
        .into_iter()
        .map(|(rel, text)| (format!("src/{rel}"), text))
        .collect();
    super::nonvacuity::require_enumerated(raw.len(), 100, "src files", "src/ moved");
    let found = blanket_dead_code_allows(&raw);
    assert!(
        found.len() <= SRC_BLANKET_ALLOW_CEILING,
        "{} blanket #![allow(dead_code)] in src/, ceiling {SRC_BLANKET_ALLOW_CEILING}: delete the dead item instead:\n{}",
        found.len(),
        found.join("\n")
    );
}

#[test]
fn blanket_dead_code_allows_in_tests_common_never_grow() {
    let raw = common_files();
    super::nonvacuity::require_enumerated(
        raw.len(),
        10,
        "tests/common files",
        "tests/common moved",
    );
    let found = blanket_dead_code_allows(&raw);
    assert!(
        found.len() <= COMMON_BLANKET_ALLOW_CEILING,
        "{} blanket #![allow(dead_code)] in tests/common, ceiling {COMMON_BLANKET_ALLOW_CEILING}:\n{}",
        found.len(),
        found.join("\n")
    );
}

#[test]
fn duplicate_function_bodies_in_src_never_grow() {
    let files = src_products();
    super::nonvacuity::require_enumerated(files.len(), 100, "src product files", "src/ moved");
    let groups = duplicate_bodies(&files, DUPLICATE_MIN_LINES);
    let listing: Vec<String> = groups.iter().map(|g| g.join(" = ")).collect();
    assert!(
        groups.len() <= DUPLICATE_BODY_CEILING,
        "{} duplicate function-body groups, ceiling {DUPLICATE_BODY_CEILING}: share one body:\n{}",
        groups.len(),
        listing.join("\n")
    );
}

/// An in-memory text tree.
fn tree(files: &[(&str, &str)]) -> Vec<(String, String)> {
    files
        .iter()
        .map(|(r, t)| (r.to_string(), t.to_string()))
        .collect()
}

#[test]
fn an_unreferenced_python_module_is_an_orphan_and_a_referenced_or_run_one_is_not() {
    let files = tree(&[
        ("dev/pytools/lonely.py", "def f():\n    return 1\n"),
        (
            "dev/pytools/runs.py",
            "def f():\n    pass\nif __name__ == \"__main__\":\n    f()\n",
        ),
        ("dev/pytools/by_path.py", "x = 1\n"),
        ("dev/pytools/by_dotted.py", "x = 1\n"),
        ("dev/pytools/by_import.py", "x = 1\n"),
        ("dev/pytools/test_thing.py", "x = 1\n"),
        (
            "Makefile",
            "go:\n\tpython3 dev/pytools/by_path.py\n\tpython3 -m dev.pytools.by_dotted\n",
        ),
        (
            "dev/pytools/user.py",
            "from by_import import x\nif __name__ == \"__main__\":\n    x\n",
        ),
    ]);
    assert_eq!(python_orphans(&files), vec!["dev/pytools/lonely.py"]);
}

#[test]
fn a_module_naming_itself_is_still_an_orphan() {
    let files = tree(&[(
        "dev/pytools/solo.py",
        "# dev/pytools/solo.py\nimport solo\n",
    )]);
    assert_eq!(python_orphans(&files), vec!["dev/pytools/solo.py"]);
}

#[test]
fn a_blanket_allow_is_found_and_an_item_allow_or_another_lint_is_not() {
    let files = tree(&[
        ("a.rs", "#![allow(dead_code)]\nfn f() {}\n"),
        ("b.rs", "#![allow(dead_code, unused_imports)]\n"),
        ("c.rs", "#[allow(dead_code)]\nfn f() {}\n"),
        ("d.rs", "#![allow(unused_imports)]\n"),
        (
            "e.rs",
            "// #![allow(dead_code)]\nconst S: &str = \"#![allow(dead_code)]\";\n",
        ),
    ]);
    assert_eq!(blanket_dead_code_allows(&files), vec!["a.rs:1", "b.rs:1"]);
}

#[test]
fn a_body_repeated_across_files_is_a_group_and_one_in_a_single_file_or_a_short_one_is_not() {
    let body = "let a = 1;\nlet b = 2;\nlet c = a + b;\nlet d = c * 2;\nlet e = d - 1;\nlet f = e;\nlet g = f;\nlet h = g;\nh\n";
    let f = |name: &str| format!("fn {name}() -> u32 {{\n{body}}}\n");
    let files = product_files(&tree(&[
        ("src/a.rs", &f("one")),
        ("src/b.rs", &f("two")),
        ("src/c.rs", &format!("{}{}", f("three"), f("four"))),
        ("src/d.rs", "fn tiny() -> u32 {\n1\n}\n"),
        ("src/e.rs", "fn tiny() -> u32 {\n1\n}\n"),
    ]));
    let groups = duplicate_bodies(&files, 8);
    assert_eq!(
        groups,
        vec![vec![
            "src/a.rs::one".to_string(),
            "src/b.rs::two".to_string(),
            "src/c.rs::four".to_string(),
            "src/c.rs::three".to_string()
        ]]
    );
    let only_c = product_files(&tree(&[(
        "src/c.rs",
        &format!("{}{}", f("three"), f("four")),
    )]));
    assert!(duplicate_bodies(&only_c, 8).is_empty());
}

#[test]
fn a_body_differing_in_one_literal_is_not_a_duplicate() {
    let mk = |n: u32| {
        format!(
            "fn f() -> u32 {{\nlet a = {n};\nlet b = 2;\nlet c = 3;\nlet d = 4;\nlet e = 5;\nlet f = 6;\nlet g = 7;\na + b + c + d + e + f + g\n}}\n"
        )
    };
    let files = product_files(&tree(&[("src/a.rs", &mk(1)), ("src/b.rs", &mk(9))]));
    assert!(duplicate_bodies(&files, 8).is_empty());
}
