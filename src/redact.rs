//! **Layer: Cross-cutting helper** (credential redaction invariant, v0.7.2 P0.3)
//!
//! Single chokepoint for stripping plaintext credential material out of
//! strings that are about to land in operator-visible artifacts: logs,
//! `summary.json` / `summary.md`, the run journal, Slack/webhook payloads,
//! and hard-failure error messages bubbling out of any subcommand.
//!
//! The invariant this module backs:
//!
//! > A credential that the operator passed through `password`,
//! > `*_env`, `*_file`, `credentials_file`, or as an embedded
//! > `user:password@host` URL MUST NOT round-trip into any persisted or
//! > emitted artifact.  When in doubt, redact.
//!
//! Scope:
//! - **Embedded-URL passwords**: `scheme://user:password@host…` →
//!   `scheme://REDACTED@host…`.  This is the only pattern Rivet
//!   round-trips through driver/error context, so it is the single
//!   high-value rewrite.  Patches expand here.
//! - **Keyword/value passwords**: `host=h password=secret dbname=d` (libpq),
//!   `Server=h;Password=secret;` / `PWD=secret` (ADO) and `;password=secret`
//!   (JDBC properties) → `password=***`.
//! - **Known token-shape secrets** (AWS access keys etc.) are *not*
//!   matched on shape today — they shouldn't be in stringified error
//!   context unless the operator passed `--source 'aws_access_key_id=AKIA…'`
//!   by mistake.  If a leak vector is discovered, add it here, write a
//!   regression test, and roll a patch release.
//!
//! What this module does NOT guarantee (documented in [`SECURITY.md`]):
//! - Third-party driver/library output that bypasses our error wrappers.
//! - In-memory secrets — every credential slot holds a [`crate::secret::Secret`],
//!   but anything copied into a `String` along the way may
//!   linger in process memory until allocator reuse.
//! - Secrets the operator captured *outside* Rivet (shell history, env
//!   var dumps, `ps` snapshots) — out of scope.

/// Replace `user:password@host` userinfo segments in any URL-like
/// substring with `REDACTED@host`.
///
/// Conservative match:
/// - scheme is `[A-Za-z][A-Za-z0-9+.\-]*`
/// - followed by `://`
/// - then a userinfo run of non-whitespace characters containing `:`
///   (i.e. `user:password`)
/// - terminated by the last `@` before whitespace
///
/// A bare `user@host` (no `:`) is preserved verbatim — there's no
/// password to redact, and stripping the username makes log lines
/// harder to triage.  Operators wanting full userinfo redaction can
/// continue to rely on `SourceConfig::redact_for_artifact` for the
/// structural path.
///
/// Idempotent: once-redacted strings pass through unchanged.
pub fn redact_url_passwords(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for word in s.split_inclusive(|c: char| c.is_ascii_whitespace()) {
        match crate::url_text::credential_span(word, crate::url_text::Mask::Password)
            .and_then(|(start, at)| word.get(..start).zip(word.get(at..)))
        {
            Some((scheme, host)) => {
                out.push_str(scheme);
                out.push_str("REDACTED");
                out.push_str(host);
            }
            None => out.push_str(word),
        }
    }
    out
}

/// Compose every redactor.  Use this at every boundary that turns a
/// driver/library error (or any operator-untrusted string) into a
/// persisted or emitted artifact.
pub fn redact_secrets(s: &str) -> String {
    redact_query_secrets(&redact_url_passwords(&redact_keyword_passwords(s)))
}

/// `raw` with its keyword/value passwords, its whole userinfo (user included) and its query secrets masked, and whether any was found: the form a persisted plan holds.
pub(crate) fn redact_artifact_url(raw: &str) -> (String, bool) {
    let keyword = redact_keyword_passwords(raw);
    let span = crate::url_text::credential_span(&keyword, crate::url_text::Mask::AnyUserinfo);
    let userinfo = match span {
        Some((start, at)) => format!("{}REDACTED{}", &keyword[..start], &keyword[at..]),
        None => keyword.clone(),
    };
    let query = redact_query_secrets(&userinfo);
    let changed = keyword != raw || span.is_some() || query != userinfo;
    (query, changed)
}

/// The marker a masked keyword/value password is replaced with.
const MASK: &str = "***";

/// Mask the value of every `password=` / `pwd=` pair of a keyword/value connection string (libpq DSN, ADO, `;`-separated JDBC properties).
pub fn redact_keyword_passwords(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut stops = s.match_indices([';', '\n', '\r']).peekable();
    let mut copied = 0;
    for (key_start, value_start) in password_pairs(s) {
        if key_start < copied {
            continue;
        }
        while stops.next_if(|(at, _)| *at < value_start).is_some() {}
        let stop = stops.peek().map_or(s.len(), |(at, _)| *at);
        out.push_str(&s[copied..value_start]);
        out.push_str(MASK);
        copied = value_start + keyword_value_len(s, key_start, value_start, stop);
    }
    out.push_str(&s[copied..]);
    out
}

/// Whether `s` holds a keyword/value password pair, masked or not.
pub(crate) fn has_keyword_password(s: &str) -> bool {
    password_pairs(s).next().is_some()
}

/// A byte a keyword may be spelled with (`db.password`, `MYSQL_PWD`, `x-passwd`).
fn is_keyword_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b'-')
}

/// Whether a keyword names a password: it ends in `password`, `passwd` or `pwd`, in any case.
fn is_password_keyword(key: &str) -> bool {
    ["password", "passwd", "pwd"].iter().any(|word| {
        key.len() >= word.len()
            && seeded!(
                "redact-key-cut-at-byte-offset",
                key[key.len() - word.len()..].eq_ignore_ascii_case(word),
                key.as_bytes()[key.len() - word.len()..].eq_ignore_ascii_case(word.as_bytes())
            )
    })
}

/// Every `keyword = value` pair whose keyword names a password, as (keyword start, value start), one per `=` in text order; a URL query pair (`?password=`, `&password=`) is left to [`redact_query_secrets`].
fn password_pairs(s: &str) -> impl Iterator<Item = (usize, usize)> + '_ {
    let bytes = s.as_bytes();
    let is_blank = |b: &&u8| matches!(**b, b' ' | b'\t');
    s.match_indices('=').filter_map(move |(eq, _)| {
        let key_end = eq - bytes[..eq].iter().rev().take_while(is_blank).count();
        let key_start = key_end
            - bytes[..key_end]
                .iter()
                .rev()
                .take_while(|b| is_keyword_byte(**b))
                .count();
        let in_query = key_start > 0 && matches!(bytes[key_start - 1], b'?' | b'&');
        let value_start = eq + 1 + bytes[eq + 1..].iter().take_while(is_blank).count();
        (!in_query && is_password_keyword(&s[key_start..key_end]))
            .then_some((key_start, value_start))
    })
}

/// Length of the value starting at `value_start`, given the first `;` or line break at or after it (`stop`): the longer of its quoted and its unquoted reading, so a quote character that is not a quote in the string's grammar leaks nothing.
fn keyword_value_len(s: &str, key_start: usize, value_start: usize, stop: usize) -> usize {
    let rest = &s[value_start..];
    let quoted = match rest.chars().next() {
        Some('\'') => quoted_value_len(rest, '\''),
        Some('"') => quoted_value_len(rest, '"'),
        Some('{') => quoted_value_len(rest, '}'),
        _ => 0,
    };
    let after_semicolon = s[..key_start].trim_end_matches([' ', '\t']).ends_with(';');
    quoted.max(bare_value_len(rest, stop - value_start, after_semicolon))
}

/// Length of an unquoted value whose first `;` or line break is at `stop`: up to `stop` in a `;`-separated string, else to whichever of the libpq (whitespace) and ADO (`;`) terminators comes later, so neither grammar leaks a tail.
fn bare_value_len(rest: &str, stop: usize, after_semicolon: bool) -> usize {
    let ends_value = |c: char| c == ';' || c.is_whitespace();
    let masked = rest
        .strip_prefix(MASK)
        .is_some_and(|after| after.chars().next().is_none_or(ends_value));
    if masked {
        return MASK.len();
    }
    if after_semicolon {
        return stop;
    }
    let semicolon = if rest[stop..].starts_with(';') {
        stop
    } else {
        0
    };
    unescaped_whitespace(rest).max(semicolon)
}

/// Length of a quoted value including both quotes, honouring `\x` escapes and a doubled closing quote; the whole text when the quote never closes.
fn quoted_value_len(rest: &str, close: char) -> usize {
    let mut chars = rest.char_indices().skip(1).peekable();
    while let Some((i, c)) = chars.next() {
        if c == '\\' {
            chars.next();
        } else if c == close {
            if chars.peek().is_some_and(|(_, next)| *next == close) {
                chars.next();
            } else {
                return i + close.len_utf8();
            }
        }
    }
    rest.len()
}

/// Offset of the first whitespace not escaped by a backslash, or the length of `text`; a backslash never escapes a line break.
fn unescaped_whitespace(text: &str) -> usize {
    let mut chars = text.char_indices().peekable();
    while let Some((i, c)) = chars.next() {
        if c == '\\' {
            chars.next_if(|(_, next)| !matches!(next, '\n' | '\r'));
        } else if c.is_whitespace() {
            return i;
        }
    }
    text.len()
}

/// Mask the value of every secret-bearing URL query pair (`?token=`, `&client_secret=`, `sig=`, `sas=`, and any key [`is_password_keyword`] accepts), keeping the key's spelling; a percent-encoded key is read decoded.
pub(crate) fn redact_query_secrets(s: &str) -> String {
    const FAMILIES: [&str; 7] = [
        "token",
        "apikey",
        "api_key",
        "api-key",
        "secret",
        "signature",
        "sig",
    ];
    fn is_secret_key(key: &str) -> bool {
        let lower = key.to_ascii_lowercase();
        is_password_keyword(key)
            || FAMILIES.iter().any(|f| {
                lower == *f
                    || (lower.ends_with(f) && lower[..lower.len() - f.len()].ends_with(['_', '-']))
            })
            || lower == "sas"
    }
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(q) = rest.find(['?', '&']) {
        out.push_str(&rest[..=q]);
        rest = &rest[q + 1..];
        let key_end = rest
            .find(|c: char| !c.is_ascii() || !(is_keyword_byte(c as u8) || c == '%'))
            .unwrap_or(rest.len());
        let key = percent_encoding::percent_decode_str(&rest[..key_end]).decode_utf8_lossy();
        if key_end < rest.len() && rest.as_bytes()[key_end] == b'=' && is_secret_key(&key) {
            out.push_str(&rest[..key_end]);
            out.push_str("=***");
            let after = &rest[key_end + 1..];
            let val_end = after
                .find(|c: char| matches!(c, '&' | '\'' | '"' | ')' | ';') || c.is_whitespace())
                .map(|i| key_end + 1 + i)
                .unwrap_or(rest.len());
            rest = &rest[val_end..];
        }
    }
    out.push_str(rest);
    out
}

/// Neutralise terminal-control bytes in text that originates outside the
/// renderer (DB error strings forwarded over IPC, captured child stderr)
/// before it is written to the operator's terminal.
///
/// A malicious or compromised source DB can embed ANSI/OSC escape sequences
/// in an error message (e.g. `ESC ] 0 ; … BEL` to rewrite the window title,
/// `ESC [ 2 J` to clear the screen). Those reach us verbatim through the
/// error-card / child-stderr render paths, so every such string is funnelled
/// through here first (CWE-150). Each dangerous character is replaced with an
/// ASCII-safe `\u{NN}` escape rather than dropped, so the operator still sees
/// that bytes were present without the terminal interpreting them.
///
/// Stripped: the C0 controls `U+0000..=U+001F` (except TAB and LF, the only
/// layout whitespace the renderer relies on — CR is removed because it would
/// reposition the cursor), DEL `U+007F`, and the C1 controls
/// `U+0080..=U+009F`. Operating on `char`s means multi-byte display glyphs
/// (`✓`, `…`, `─`) are preserved intact — their UTF-8 continuation bytes are
/// never examined in isolation.
pub(crate) fn sanitize_terminal(text: &str) -> String {
    let needs_escape = |c: char| {
        let cp = c as u32;
        (cp <= 0x1f && c != '\t' && c != '\n') || cp == 0x7f || (0x80..=0x9f).contains(&cp)
    };
    if !text.chars().any(needs_escape) {
        return text.to_string();
    }
    let mut out = String::with_capacity(text.len());
    for c in text.chars() {
        if needs_escape(c) {
            out.push_str(&format!("\\u{{{:02x}}}", c as u32));
        } else {
            out.push(c);
        }
    }
    out
}

/// The scheduler's error object for one failed unit (ADR-0039 D1): the one function every emitter calls.
pub fn error_object(err: &anyhow::Error) -> serde_json::Value {
    use crate::error::{ExitClass, classify_exit, codes, error_code};
    let class = ExitClass::from_code(classify_exit(err)).unwrap_or(ExitClass::Generic);
    let code = error_code(err).and_then(|id| codes::ALL.iter().find(|c| c.id == id));
    serde_json::json!({
        "code": code.map(|c| c.id),
        "kind": code.map(|c| c.kind.name()),
        "class": class.name(),
        "exit_code": class.code(),
        "retryable": class == ExitClass::Retryable,
        "action": code.map(|c| c.action),
        "message": sanitize_terminal(&redact_error(err)),
    })
}

/// Convenience: format an `anyhow::Error` with `{:#}` and redact the
/// result in one call.  Use at the boundary of every error-to-artifact
/// path (`summary.error_message = ...`, `log::error!(... e ...)`).
pub fn redact_error(e: &anyhow::Error) -> String {
    redact_secrets(&format!("{e:#}"))
}

/// Render one log record into a redacted, operator-visible line.
///
/// The module scope names **logs** as a redaction target, but the `log::*`
/// macros bypass the artifact-path redaction that is wired by hand at the
/// error/summary call sites — a `log::warn!("…{e}", e)` whose `e` captured a
/// `scheme://user:password@host` connect error would otherwise print the
/// password to stderr. `main`'s `env_logger` formatter delegates here so the
/// log **sink** itself is the chokepoint: every line, present and future,
/// passes through [`redact_secrets`] with no reliance on each call site
/// remembering to redact. Kept in this module (beside the other redactors) and
/// log-crate-agnostic (`level` is a pre-rendered `&str`) so the wiring is
/// unit-testable without capturing global stderr.
pub fn redacted_log_line(timestamp: &str, level: &str, target: &str, message: &str) -> String {
    redact_secrets(&format!("[{timestamp} {level} {target}] {message}"))
}

/// Install the process logger: `env_logger` on stderr (default `warn`), every
/// line through [`redacted_log_line`], and — while an in-process card renderer
/// owns the screen — routed through its channel so the line lands above the
/// card block instead of between two frames (which duplicated the block).
/// `route` is that channel, handed in by the layer that owns it.
pub fn install_logger(route: LogRoute) {
    use std::io::Write as _;
    let stderr_logger =
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("warn"))
            .format(|buf, record| {
                let line = redacted_log_line(
                    &buf.timestamp().to_string(),
                    record.level().as_str(),
                    record.target(),
                    &record.args().to_string(),
                );
                writeln!(buf, "{line}")
            })
            .build();
    log::set_max_level(stderr_logger.filter());
    // A second install (a test process that already has a logger) keeps the
    // first; `main` calls this exactly once.
    let _ = log::set_boxed_logger(Box::new(UiRoutedLogger(stderr_logger, route)));
}

/// ERROR-level lines this process has logged.
static ERRORS_LOGGED: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// How many ERROR-level lines this process has logged, so a command that reports problems as log lines can refuse to call itself green.
pub fn errors_logged() -> usize {
    ERRORS_LOGGED.load(std::sync::atomic::Ordering::Relaxed)
}

/// Takes a formatted, redacted log line; `false` leaves it to stderr.
pub type LogRoute = fn(String) -> bool;

struct UiRoutedLogger(env_logger::Logger, LogRoute);

impl log::Log for UiRoutedLogger {
    fn enabled(&self, metadata: &log::Metadata) -> bool {
        self.0.enabled(metadata)
    }

    fn log(&self, record: &log::Record) {
        if !self.0.matches(record) {
            return;
        }
        if record.level() == log::Level::Error {
            ERRORS_LOGGED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        let line = redacted_log_line(
            &chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
            record.level().as_str(),
            record.target(),
            &record.args().to_string(),
        );
        if !(self.1)(line) {
            self.0.log(record);
        }
    }

    fn flush(&self) {
        self.0.flush();
    }
}

#[cfg(test)]
mod logger_tests {
    use super::*;
    use log::Log as _;
    use std::sync::{Arc, Mutex};

    /// An `env_logger` at `warn` whose stderr is this buffer, routed as the binary routes it.
    fn logger(sink: Arc<Mutex<Vec<u8>>>) -> UiRoutedLogger {
        struct Sink(Arc<Mutex<Vec<u8>>>);
        impl std::io::Write for Sink {
            fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(b);
                Ok(b.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let inner = env_logger::Builder::new()
            .filter_level(log::LevelFilter::Warn)
            .target(env_logger::Target::Pipe(Box::new(Sink(sink))))
            .build();
        UiRoutedLogger(inner, crate::cli::LOG_ROUTE)
    }

    /// Every ERROR line the logger writes is counted, a WARN line is not.
    #[test]
    fn the_logger_counts_its_error_lines() {
        // Logs through the routed logger, so it must not run while another test owns the in-process tx.
        let _serial = crate::pipeline::parent_ui::retry_counter_test_lock()
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        let l = logger(Arc::new(Mutex::new(Vec::new())));
        let before = errors_logged();
        l.log(&record(log::Level::Error, format_args!("one")));
        l.log(&record(log::Level::Error, format_args!("two")));
        assert!(errors_logged() >= before + 2, "two ERROR lines counted");
    }

    fn record<'a>(level: log::Level, args: std::fmt::Arguments<'a>) -> log::Record<'a> {
        log::Record::builder()
            .level(level)
            .target("rivet::t")
            .args(args)
            .build()
    }

    /// The renderer's channel gets the line (formatted + redacted) while one is
    /// installed and stderr stays silent; with none, stderr gets it. A record
    /// under the filter reaches neither.
    #[test]
    fn a_log_line_goes_to_the_renderer_when_one_owns_the_screen_else_to_stderr() {
        // The in-process tx is one global; every test that installs one holds this.
        let _serial = crate::pipeline::parent_ui::retry_counter_test_lock()
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        let sink = Arc::new(Mutex::new(Vec::new()));
        let lg = logger(sink.clone());
        assert!(lg.enabled(&log::Metadata::builder().level(log::Level::Warn).build()));
        assert!(!lg.enabled(&log::Metadata::builder().level(log::Level::Info).build()));

        let (tx, rx) = std::sync::mpsc::channel();
        crate::pipeline::ipc::install_in_process_tx(tx);
        lg.log(&record(
            log::Level::Warn,
            format_args!("slow mysql://u:pw@h/db"),
        ));
        lg.log(&record(log::Level::Info, format_args!("chatter")));
        crate::pipeline::ipc::clear_in_process_tx();
        let routed: Vec<String> = rx
            .try_iter()
            .map(|m| match m {
                crate::pipeline::parent_ui::UiMessage::Log(l) => l,
                other => panic!("not a log line: {other:?}"),
            })
            .collect();
        assert_eq!(
            routed.len(),
            1,
            "the WARN routed, the INFO filtered: {routed:?}"
        );
        assert!(
            routed[0].contains("WARN rivet::t] slow") && !routed[0].contains("pw@"),
            "formatted through the redacting line: {}",
            routed[0]
        );
        assert!(
            sink.lock().unwrap().is_empty(),
            "nothing reached stderr while the renderer owned the screen"
        );

        lg.log(&record(
            log::Level::Warn,
            format_args!("after the renderer"),
        ));
        let stderr = String::from_utf8(sink.lock().unwrap().clone()).unwrap();
        assert!(stderr.contains("after the renderer"), "{stderr:?}");
    }
}

#[cfg(test)]
mod tests {

    proptest::proptest! {
        #![proptest_config(proptest::prelude::ProptestConfig {
            cases: 256, ..Default::default()
        })]

        /// The shipped contract, both directions. POSITIVE: a key that IS a
        /// family word, `sas`, or a compound joined by `_`/`-` (access_token,
        /// X-Amz-Signature) is redacted — the secret never survives, non-secret
        /// pairs do, and a second pass changes nothing (idempotence: a matcher
        /// re-triggering on its own `***` would corrupt). NEGATIVE: a GLUED
        /// prefix (`atoken`) is deliberately NOT matched — round-10 chose the
        /// separator boundary to keep diagnostics like Azure's `sv` readable,
        /// and this pins that choice so a future "fix" widening the match is a
        /// conscious contract change, not a drive-by.
        #[test]
        fn suffix_family_contract_redacts_compounds_and_spares_glued_keys(
            family in proptest::sample::select(vec![
                "token", "password", "apikey", "api_key", "api-key",
                "secret", "signature", "sig",
            ]),
            prefix in "[a-z]{1,8}",
            sep in proptest::sample::select(vec!["_", "-"]),
            exact in proptest::prelude::any::<bool>(),
            use_sas in proptest::prelude::any::<bool>(),
            secret in "[A-Za-z0-9]{8,24}",
            tail_sep in proptest::sample::select(vec!["&", ";", "'", "\""]),
        ) {
            // POSITIVE: exact family word, exact `sas`, or a separator-joined
            // compound. All must redact.
            let key = if exact {
                if use_sas { "sas".to_string() } else { family.to_string() }
            } else {
                format!("{prefix}{sep}{family}")
            };
            let text = format!("postgresql://u@h/db?keep=1&{key}={secret}{tail_sep}tail=x");
            let once = redact_query_secrets(&text);
            proptest::prop_assert!(
                !once.contains(&secret),
                "secret `{}` survived redaction of `{}` -> `{}`", secret, text, once
            );
            proptest::prop_assert!(
                once.contains("keep=1") && once.contains("tail=x"),
                "non-secret pairs must survive: `{}` -> `{}`", text, once
            );
            let twice = redact_query_secrets(&once);
            proptest::prop_assert_eq!(&once, &twice, "redaction must be idempotent");

            // NEGATIVE: the same family GLUED to the prefix (no separator) is
            // not a match — the documented FP-avoidance boundary.
            let glued = format!("postgresql://u@h/db?{prefix}{family}={secret}");
            proptest::prop_assume!(family != "password");
            proptest::prop_assert_eq!(
                redact_query_secrets(&glued),
                glued.clone(),
                "a glued key must pass through untouched (the separator boundary \
                 is the contract)"
            );
        }
    }
    /// Round-8: query-param secrets are scrubbed (the Slack-path find made
    /// this boundary load-bearing). Values after token/password/api_key/
    /// secret/sig/sas/sv become ***; foreign keys and non-URLs untouched.
    #[test]
    fn query_param_secrets_are_scrubbed() {
        assert_eq!(
            redact_secrets("GET https://h/hook?token=top-secret failed"),
            "GET https://h/hook?token=*** failed"
        );
        assert_eq!(
            redact_secrets("postgres://h/db?password=hunter2&sslmode=require"),
            "postgres://h/db?password=***&sslmode=require"
        );
        // `sv` deliberately NOT redacted since round-10 (service VERSION);
        // `sig` — the actual SAS credential — is.
        assert_eq!(
            redact_secrets("https://acc.blob/x?sv=2020&sig=abc%2F=="),
            "https://acc.blob/x?sv=2020&sig=***"
        );
        // Compound families (round-10): the dominant real-world shapes.
        for (input, want) in [
            ("?access_token=abc&x=1", "?access_token=***&x=1"),
            (
                "?X-Amz-Signature=deadbeef&X-Amz-Date=2026",
                "?X-Amz-Signature=***&X-Amz-Date=2026",
            ),
            ("&client_secret=s3cr3t ", "&client_secret=*** "),
            ("&sas_token=tok\"", "&sas_token=***\""),
            // reqwest's rendering: the paren must survive OUTSIDE the span.
            (
                "(https://h/hook?token=abc): connect error",
                "(https://h/hook?token=***): connect error",
            ),
            // Azure's service VERSION is diagnostic, not a secret.
            ("?sv=2020-08-04&sig=abc", "?sv=2020-08-04&sig=***"),
        ] {
            assert_eq!(redact_secrets(input), want, "{input}");
        }
        // A non-secret key and plain text stay byte-identical.
        assert_eq!(
            redact_secrets("https://h/x?page=2&limit=10"),
            "https://h/x?page=2&limit=10"
        );
        assert_eq!(redact_secrets("no urls here"), "no urls here");
    }

    use super::*;

    // ── URL-safety matrix: the recurring credential-leak class, at the infra
    //    level (docs/url-safety-matrix.yaml). The class has regressed THREE times
    //    (round-1 MSSQL round-trip, round-3 the state-URL redactor, and the general redactor
    //    here), each invisible to point tests. This data-driven test sweeps every
    //    scheme × hostile-userinfo shape through the general log redactor and
    //    asserts the secret NEVER survives. Adding a URL shape here is cheaper than
    //    another audit round; a redaction change that reopens the leak fails CI. ──
    #[test]
    fn redact_secrets_url_safety_matrix() {
        const SECRET: &str = "S3cr3tPw";
        // Every engine scheme rivet builds/logs.
        let schemes = ["postgresql", "mysql", "sqlserver", "mongodb", "oracle"];
        // Password bodies embedding the secret + a URL delimiter that has
        // historically defeated a redactor (a raw '/' is in the base64 alphabet;
        // '@'-before-'/' defeated round-3's fail-safe; ':' defeated the rfind split).
        let bodies = [
            SECRET.to_string(),
            format!("{SECRET}/x"),
            format!("{SECRET}?x"),
            format!("{SECRET}#x"),
            format!("{SECRET}@x"),
            format!("{SECRET}:x"),
            format!("pre/{SECRET}"),
            format!("a/b?c#{SECRET}"),
            format!("Kp@{SECRET}/x"), // '@' BEFORE '/' (round-4)
            format!("Kp@{SECRET}?x"),
            format!("Kp@{SECRET}#x"),
            format!("a:b:{SECRET}"), // ':'-bearing password (round-4)
        ];
        for scheme in schemes {
            for body in &bodies {
                // A plain URL, AND a URL with a stray '@' in the query after the host
                // (the RFC-legal ?opt=a@b shape — the round-4 stray-@ coverage gap).
                for url in [
                    format!("{scheme}://user:{body}@host:5432/db"),
                    format!("{scheme}://user:{body}@host:5432/db?opt=a@b"),
                ] {
                    // Bare, inside an error prefix, and mid-log-line (the three real
                    // shapes a URL reaches the sink in).
                    for ctx in [
                        url.clone(),
                        format!("connect to {url} failed: timeout"),
                        format!("[2026-07-22 ERROR src] dialing {url} then EOF"),
                    ] {
                        let out = redact_secrets(&ctx);
                        assert!(
                            !out.contains(SECRET),
                            "SECRET leaked through redact_secrets\n  scheme={scheme} body={body}\n  in:  {ctx}\n  out: {out}"
                        );
                    }
                }
            }
        }
    }

    // ── redact_keyword_passwords (docs/url-safety-matrix.yaml) ─────────────

    /// Every keyword/value form and what it becomes, byte for byte.
    const KEYWORD_FORMS: [(&str, &str); 22] = [
        (
            "host=h port=5432 user=u password=S3cr3tPw dbname=d",
            "host=h port=5432 user=u password=*** dbname=d",
        ),
        (
            "host=h user=u password=S3cr3tPw",
            "host=h user=u password=***",
        ),
        (
            "host=h password = S3cr3tPw dbname=d",
            "host=h password = *** dbname=d",
        ),
        (
            r"host=h password='S3cr3tPw it\'s S3cr3tPw' dbname=d",
            "host=h password=*** dbname=d",
        ),
        (
            r"host=h password=S3cr3tPw\ S3cr3tPw dbname=d",
            "host=h password=*** dbname=d",
        ),
        (
            "host=h password=a;S3cr3tPw dbname=d",
            "host=h password=*** dbname=d",
        ),
        (
            "host=h password='S3cr3tPw never closed",
            "host=h password=***",
        ),
        (
            "Server=h,1433;Database=d;User Id=u;Password=S3cr3tPw;",
            "Server=h,1433;Database=d;User Id=u;Password=***;",
        ),
        (
            "Server=h;UID=u;PWD=S3cr3tPw;Encrypt=true",
            "Server=h;UID=u;PWD=***;Encrypt=true",
        ),
        (
            "Server=h;User Id=u;Password=two S3cr3tPw words;Encrypt=true",
            "Server=h;User Id=u;Password=***;Encrypt=true",
        ),
        (
            "Password=two S3cr3tPw words;Server=h",
            "Password=***;Server=h",
        ),
        (
            "Server=h;Password=S3cr3tPw trailing S3cr3tPw",
            "Server=h;Password=***",
        ),
        (
            "Server=h;Password=\"a;S3cr3tPw\"\"S3cr3tPw\";Encrypt=true",
            "Server=h;Password=***;Encrypt=true",
        ),
        (
            "Driver={x};Server=h;PWD={a;S3cr3tPw}}S3cr3tPw};Encrypt=yes",
            "Driver={x};Server=h;PWD=***;Encrypt=yes",
        ),
        (
            "sqlserver://h:1433;databaseName=app;user=sa;password=S3cr3tPw",
            "sqlserver://h:1433;databaseName=app;user=sa;password=***",
        ),
        (
            "sqlserver://h:1433;user=sa;password=p@S3cr3tPw;encrypt=true",
            "sqlserver://h:1433;user=sa;password=***;encrypt=true",
        ),
        ("PGPASSWORD=S3cr3tPw psql", "PGPASSWORD=*** psql"),
        (
            "spring.datasource.password=S3cr3tPw",
            "spring.datasource.password=***",
        ),
        (
            "MYSQL_PWD=S3cr3tPw x-passwd=S3cr3tPw",
            "MYSQL_PWD=*** x-passwd=***",
        ),
        (
            "host=a password=S3cr3tPw\nhost=b password=S3cr3tPw dbname=d",
            "host=a password=***\nhost=b password=*** dbname=d",
        ),
        (
            "nothing is listening on host=h port=1 password=S3cr3tPw dbname=d — check the port; then retry",
            "nothing is listening on host=h port=1 password=***; then retry",
        ),
        (
            "Server=h;Password=;Encrypt=true",
            "Server=h;Password=***;Encrypt=true",
        ),
    ];

    #[test]
    fn keyword_value_passwords_are_masked_in_every_form() {
        for (input, want) in KEYWORD_FORMS {
            assert_eq!(redact_keyword_passwords(input), want, "{input}");
            let through_all = redact_secrets(input);
            assert_eq!(through_all, want, "redact_secrets: {input}");
            assert_eq!(
                redact_secrets(&through_all),
                through_all,
                "a second pass must change nothing: {input}"
            );
            assert!(has_keyword_password(input), "{input}");
        }
    }

    /// Text main already handled, which the keyword/value pass must leave byte-identical.
    #[test]
    fn text_without_a_keyword_password_pair_is_untouched() {
        for s in [
            "host=h port=5432 user=u dbname=d sslmode=require",
            "Server=h;Database=d;Integrated Security=true;",
            "postgres://h/db?password=hunter2&sslmode=require",
            "postgresql://u@h/db?keep=1&db_password=x&tail=1",
            "the password is wrong",
            "passwords=3 password_env=PGPASS password: x",
            "mode=keyset chunk_size=1000",
            "naïve — host=h dbname=d",
        ] {
            assert_eq!(redact_keyword_passwords(s), s, "{s}");
            assert!(!has_keyword_password(s), "{s}");
        }
        assert_eq!(
            redact_secrets("postgres://h/db?password=hunter2&sslmode=require"),
            "postgres://h/db?password=***&sslmode=require",
            "a query pair still ends at `&`"
        );
        assert_eq!(
            redact_secrets("postgresql://u:pw@h:5432/db and host=h password=x dbname=d"),
            "postgresql://REDACTED@h:5432/db and host=h password=*** dbname=d"
        );
    }

    /// The secret never survives, whatever it is made of and wherever the string lands.
    #[test]
    fn redact_secrets_keyword_value_matrix() {
        const SECRET: &str = "S3cr3tPw";
        let bodies = [
            SECRET.to_string(),
            format!("{SECRET}@x"),
            format!("a@{SECRET}"),
            format!("{SECRET}/x?y#z"),
            format!("a=b&{SECRET}"),
            format!("a:{SECRET}:b"),
            format!("\"{SECRET}"),
            format!("a'{SECRET}"),
            format!("a{{{SECRET}"),
            format!("a,{SECRET})"),
        ];
        for body in &bodies {
            let quoted = body.replace('\'', r"\'");
            for conn in [
                format!("host=h port=5432 user=u password={body} dbname=d"),
                format!("host=h user=u dbname=d password={body}"),
                format!("host=h user=u password='{quoted} {quoted}' dbname=d"),
                format!("host=h user=u password={body};{body} dbname=d"),
                format!("Server=h,1433;Database=d;User Id=u;Password={body};"),
                format!("Server=h;UID=u;PWD={body} {body};Encrypt=true"),
                format!("pwd={body} {body};Server=h"),
                format!("sqlserver://h:1433;databaseName=app;user=sa;password={body}"),
                format!("sqlserver://h:1433;user=sa;PASSWORD = {body};encrypt=true"),
            ] {
                for ctx in [
                    conn.clone(),
                    format!("mssql url must start with sqlserver:// — got {conn}"),
                    format!("[2026-10-07 WARN src] dialing {conn}"),
                    format!("first line\nconnect to {conn}\nlast line"),
                ] {
                    let out = redact_secrets(&ctx);
                    assert!(
                        !out.contains(SECRET),
                        "SECRET leaked\n  body={body}\n  in:  {ctx}\n  out: {out}"
                    );
                    assert!(out.contains("***"), "nothing was masked: {out}");
                    assert_eq!(redact_secrets(&out), out, "second pass: {ctx}");
                }
            }
        }
    }

    /// The keyword/value scan of `input`; a string with no password pair must come back unchanged.
    fn keyword_scan(input: &str) -> Result<String, String> {
        let has = has_keyword_password(input);
        let out = redact_keyword_passwords(input);
        if has || out == input {
            Ok(out)
        } else {
            Err("masked a string with no password pair".to_string())
        }
    }

    /// `keyword_scan` of `input` on a hang-guarded thread.
    fn scan_within_budget(label: &str, input: &str) -> Result<String, String> {
        let owned = input.to_string();
        crate::test_budget::hang_guard(label, move || keyword_scan(&owned))
    }

    /// Unterminated quotes, a key at the very end, empty values, multi-byte text after `=`: each returns, and returns this.
    #[test]
    fn the_keyword_scan_returns_on_every_degenerate_shape() {
        for (input, want) in [
            ("password=", "password=***"),
            ("host=h password", "host=h password"),
            ("host=h password=", "host=h password=***"),
            ("host=h password= ", "host=h password= ***"),
            ("host=h password=\t\t", "host=h password=\t\t***"),
            ("password='never closed", "password=***"),
            ("password=\"never closed", "password=***"),
            ("pwd={never closed", "pwd=***"),
            ("password='", "password=***"),
            ("password=\\", "password=***"),
            ("password='\\", "password=***"),
            ("password=é", "password=***"),
            ("password=—x y", "password=*** y"),
            ("password=😀;x", "password=***"),
            ("naïve=x password=日本;", "naïve=x password=***"),
            ("=", "="),
            ("==", "=="),
            (" = ", " = "),
            (";=;", ";=;"),
            ("password==x", "password=***"),
            ("password=pwd =y", "password=*** =y"),
            (
                "host=h password='a b'pwd=S3cr3tPw",
                "host=h password=***pwd=***",
            ),
            (
                "host=h password=a;pwd=S3cr3tPw dbname=d",
                "host=h password=*** dbname=d",
            ),
            (
                "host=h password=S3cr3tPw\\\nnext line",
                "host=h password=***\nnext line",
            ),
            (
                "host=h password=S3cr3tPw\\\r\nnext line",
                "host=h password=***\r\nnext line",
            ),
            ("password=x\r\npassword=y", "password=***\r\npassword=***"),
            ("x;password= ;y", "x;password= ***;y"),
            ("password=***", "password=***"),
            ("password=***x y", "password=*** y"),
            (
                "Server=h;Pwd=S3cr3tPw\\;tail;x=1",
                "Server=h;Pwd=***;tail;x=1",
            ),
            ("a ?password=x", "a ?password=x"),
            ("a &pwd = x y", "a &pwd = x y"),
        ] {
            assert_eq!(
                scan_within_budget(input, input).as_deref(),
                Ok(want),
                "{input:?}"
            );
        }
    }

    /// A hostile shape: its label, the input and the output the scan must give.
    type Shape = (&'static str, (String, String));

    /// The hostile shapes of keyword text, about `mb` bytes each, with the output the scan must give.
    fn keyword_shapes(mb: usize) -> Vec<(&'static str, String, String)> {
        let blanks = " ".repeat(mb);
        vec![
            ("`;=` repeated", ";=".repeat(mb / 2), ";=".repeat(mb / 2)),
            ("`=` alone", "=".repeat(mb), "=".repeat(mb)),
            (
                "`pwd=a ` repeated, no `;`",
                "pwd=a ".repeat(mb / 6),
                "pwd=*** ".repeat(mb / 6),
            ),
            (
                "`pwd=é ` repeated",
                "pwd=é ".repeat(mb / 7),
                "pwd=*** ".repeat(mb / 7),
            ),
            (
                "masked pairs repeated",
                "pwd=*** ".repeat(mb / 8),
                "pwd=*** ".repeat(mb / 8),
            ),
            (
                "blanks around every `=`",
                "pwd \t= \t".repeat(mb / 8),
                "pwd \t= \t*** \t= \t".repeat(mb / 16),
            ),
            (
                "`pwd=;` repeated",
                "pwd=;".repeat(mb / 5),
                "pwd=***".to_string(),
            ),
            (
                "`pwd=` then `;x=y` repeated",
                format!("a;pwd={}", ";x=y".repeat(mb / 4)),
                format!("a;pwd=***{}", ";x=y".repeat(mb / 4)),
            ),
            (
                "`password=` repeated",
                "password=".repeat(mb / 9),
                "password=***".to_string(),
            ),
            (
                "a quote that never closes",
                format!("pwd='{}", "a=b;".repeat(mb / 4)),
                "pwd=***".to_string(),
            ),
            (
                "an opening quote in every pair",
                "pwd='a".repeat(mb / 6),
                "pwd=***".to_string(),
            ),
            (
                "a megabyte of blanks before `=`",
                format!("pwd{blanks}=x"),
                format!("pwd{blanks}=***"),
            ),
            (
                "a megabyte keyword",
                format!("{}=x", "a".repeat(mb)),
                format!("{}=x", "a".repeat(mb)),
            ),
        ]
    }

    /// Eight times the hostile text costs about eight times the CPU, and every shape gets the exact output.
    #[test]
    fn the_keyword_scan_is_linear_on_a_hostile_megabyte() {
        use crate::test_budget::{SCALE, SMALL, linear_cost};
        for ((label, small, want_small), (_, large, want_large)) in keyword_shapes(SMALL)
            .into_iter()
            .zip(keyword_shapes(SMALL * SCALE))
        {
            let (got_small, got_large) =
                linear_cost(label, small, large, |input: &String| keyword_scan(input));
            assert_eq!(got_small, Ok(want_small), "{label}: small");
            assert_eq!(got_large, Ok(want_large), "{label}: large");
        }
    }

    // ── redact_url_passwords ───────────────────────────────────────────────

    #[test]
    fn rewrites_postgres_userinfo_with_password() {
        let s = "connection failed to postgresql://alice:s3cret@db.prod:5432/orders: timeout";
        let out = redact_url_passwords(s);
        assert!(!out.contains("s3cret"), "password must be stripped: {out}");
        assert!(
            out.contains("postgresql://REDACTED@db.prod:5432/orders"),
            "expected REDACTED@host, got: {out}",
        );
    }

    #[test]
    fn rewrites_mysql_userinfo_with_password() {
        let s = "auth error: mysql://root:hunter2@10.0.0.5:3306/billing";
        let out = redact_url_passwords(s);
        assert!(!out.contains("hunter2"));
        assert!(out.contains("mysql://REDACTED@10.0.0.5"));
    }

    #[test]
    fn preserves_bare_user_at_host_without_password() {
        // `user@host` has no password to strip; rewriting it would lose
        // useful triage signal.  Pin the conservative behaviour.
        let s = "connection: postgresql://alice@db.prod:5432/orders";
        assert_eq!(redact_url_passwords(s), s);
    }

    #[test]
    fn idempotent_on_already_redacted_string() {
        let s = "postgresql://REDACTED@db.prod:5432/orders";
        assert_eq!(redact_url_passwords(s), s);
    }

    #[test]
    fn preserves_non_url_text_with_at_sign() {
        // `email@example.com` is not a URL — must not be rewritten.
        let s = "user alice@example.com reported failure";
        assert_eq!(redact_url_passwords(s), s);
    }

    #[test]
    fn handles_multiple_urls_in_one_string() {
        let s = "primary postgresql://a:b@h1/d failed, retrying mysql://c:d@h2/d";
        let out = redact_url_passwords(s);
        assert!(!out.contains("a:b@"));
        assert!(!out.contains("c:d@"));
        assert!(out.contains("postgresql://REDACTED@h1/d"));
        assert!(out.contains("mysql://REDACTED@h2/d"));
    }

    #[test]
    fn stops_at_whitespace_in_userinfo() {
        // Userinfo cannot contain whitespace — defensive guard against
        // matching wild `://foo bar@…` substrings inside prose.
        let s = "scheme://broken token@host";
        assert_eq!(redact_url_passwords(s), s);
    }

    #[test]
    fn preserves_strings_without_urls() {
        let s = "export 'orders' failed: relation does not exist";
        assert_eq!(redact_url_passwords(s), s);
    }

    /// `unit` repeated to about `mb` bytes, as the input and as the output a scan must leave untouched.
    fn same(unit: &str, mb: usize) -> (String, String) {
        (unit.repeat(mb / unit.len()), unit.repeat(mb / unit.len()))
    }

    /// The hostile shapes of query text, about `mb` bytes each, as `(label, (input, wanted output))`.
    fn query_shapes(mb: usize) -> Vec<Shape> {
        let same = |unit: &str| same(unit, mb);
        vec![
            ("`?` alone", same("?")),
            ("`&a` repeated, no `=`", same("&a")),
            ("`&a=1` repeated, no secret", same("&a=1")),
            (
                "a megabyte key",
                (
                    format!("?{}=1", "a".repeat(mb)),
                    format!("?{}=1", "a".repeat(mb)),
                ),
            ),
            (
                "a megabyte secret key",
                (
                    format!("?{}_token=x", "a".repeat(mb)),
                    format!("?{}_token=***", "a".repeat(mb)),
                ),
            ),
            (
                "a megabyte secret",
                (
                    format!("?token={}", "x".repeat(mb)),
                    "?token=***".to_string(),
                ),
            ),
            (
                "`?token=x&` repeated",
                ("?token=x&".repeat(mb / 9), "?token=***&".repeat(mb / 9)),
            ),
        ]
    }

    /// The hostile shapes of URL text, about `mb` bytes each, as `(label, (input, wanted output))`.
    fn url_shapes(mb: usize) -> Vec<Shape> {
        let same = |unit: &str| same(unit, mb);
        vec![
            ("letters alone", same("a")),
            ("scheme characters alone", same("a1+.-")),
            ("`a://` repeated", same("a://")),
            ("`://` repeated", same("://")),
            ("`@` alone", same("@")),
            (
                "`a://b@` repeated, the next `://` its colon",
                ("a://b@".repeat(mb / 6), "a://REDACTED@".to_string()),
            ),
            ("`a://b@ ` repeated, no password", same("a://b@ ")),
            ("`1://b:c@` repeated, no scheme letter", same("1://b:c@")),
            ("multi-byte text", same("é—")),
            (
                "`a://` repeated before one userinfo",
                (
                    format!("{}u:p@h", "a://".repeat(mb / 4)),
                    "a://REDACTED@h".to_string(),
                ),
            ),
            (
                "one URL per word",
                (
                    "a://u:p@h ".repeat(mb / 10),
                    "a://REDACTED@h ".repeat(mb / 10),
                ),
            ),
            (
                "one URL, a megabyte password",
                (
                    format!("a://u:{}@h", "p@:/".repeat(mb / 4)),
                    "a://REDACTED@h".to_string(),
                ),
            ),
        ]
    }

    /// Each shape at `SMALL` and eight times that costs about eight times the CPU through `scan`, and gives the exact output.
    fn assert_scan_linear(shapes: fn(usize) -> Vec<Shape>, scan: fn(&str) -> String) {
        use crate::test_budget::{SCALE, SMALL, linear_cost};
        for ((label, (small, want_small)), (_, (large, want_large))) in
            shapes(SMALL).into_iter().zip(shapes(SMALL * SCALE))
        {
            let (got_small, got_large) =
                linear_cost(label, small, large, move |input: &String| scan(input));
            assert!(
                got_small == want_small,
                "{label}: small: {} bytes out",
                got_small.len()
            );
            assert!(
                got_large == want_large,
                "{label}: large: {} bytes out",
                got_large.len()
            );
        }
    }

    /// Hostile query text is scanned in one pass: the exact output at a CPU cost that grows with the input.
    #[test]
    fn the_query_scan_is_linear_on_a_hostile_megabyte() {
        assert_scan_linear(query_shapes, redact_query_secrets);
    }

    /// Hostile URL text is scanned in one pass: the exact output at a CPU cost that grows with the input.
    #[test]
    fn the_url_scan_is_linear_on_a_hostile_megabyte() {
        assert_scan_linear(url_shapes, redact_url_passwords);
    }

    /// The scan as specified before it became one pass: at each byte a scheme, `://`, then the last `@` before whitespace with a `:` ahead of it.
    fn reference_url_scan(s: &str) -> String {
        let bytes = s.as_bytes();
        let is_scheme = |b: &u8| b.is_ascii_alphanumeric() || matches!(b, b'+' | b'.' | b'-');
        let mut out = Vec::with_capacity(bytes.len());
        let mut i = 0;
        while i < bytes.len() {
            let scheme_end = i + 1 + bytes[i + 1..].iter().take_while(|b| is_scheme(b)).count();
            let userinfo = scheme_end + 3;
            let word_end = bytes[i..]
                .iter()
                .position(u8::is_ascii_whitespace)
                .map_or(bytes.len(), |n| i + n);
            let at = (userinfo..word_end).rev().find(|k| bytes[*k] == b'@');
            match at {
                Some(at)
                    if bytes[i].is_ascii_alphabetic()
                        && bytes[scheme_end..].starts_with(b"://")
                        && bytes[userinfo..at].contains(&b':') =>
                {
                    out.extend_from_slice(&bytes[i..userinfo]);
                    out.extend_from_slice(b"REDACTED");
                    i = at;
                }
                _ => {
                    out.push(bytes[i]);
                    i += 1;
                }
            }
        }
        String::from_utf8(out).expect("whole characters are copied or replaced by ASCII")
    }

    proptest::proptest! {
        #![proptest_config(proptest::prelude::ProptestConfig {
            cases: 4096, ..Default::default()
        })]

        /// Any text built from URL punctuation, whitespace and multi-byte characters is rewritten exactly as the byte-at-a-time scan rewrote it, and a second pass changes nothing.
        #[test]
        fn the_url_scan_matches_its_reference_on_any_text(text in "[ab1+.:/@?# \t\né—]{0,48}") {
            let once = redact_url_passwords(&text);
            proptest::prop_assert_eq!(&once, &reference_url_scan(&text), "{:?}", text);
            proptest::prop_assert_eq!(&redact_url_passwords(&once), &once, "{:?}", text);
        }

        /// A query key of percent escapes (any bytes, so any decoded text) followed by a keyword is read without a panic, and masked exactly when the keyword names a password.
        #[test]
        fn a_percent_escaped_prefix_before_a_keyword_is_read_without_a_panic(
            prefix in "(%[0-9A-Fa-f]{2}){1,3}",
            keyword in proptest::sample::select(vec!["password", "passwd", "PWD", "n", "pass"]),
        ) {
            let text = format!("?{prefix}{keyword}=x");
            let want = if keyword == "n" || keyword == "pass" { text.clone() } else { format!("?{prefix}{keyword}=***") };
            proptest::prop_assert_eq!(redact_secrets(&text), want);
        }

        /// Arbitrary UTF-8 passes every redactor without a panic, and unchanged when it holds neither an `@` nor an `=`.
        #[test]
        fn any_utf8_text_is_redacted_without_a_panic(text in "\\PC{0,64}") {
            let out = redact_secrets(&text);
            proptest::prop_assert!(text.contains(['@', '=']) || out == text, "{:?} -> {:?}", text, out);
        }
    }

    /// A percent-encoded query key that decodes to a multi-byte character is read without a panic.
    #[test]
    fn a_query_key_decoding_to_a_multibyte_character_is_read_without_a_panic() {
        for text in ["?%FFn=x", "a?b=1&%E2%82%ACn=2", "?%D0%BF=1", "&%E6%97%A5=v"] {
            assert_eq!(redact_secrets(text), text);
        }
        assert_eq!(redact_secrets("?%FFpwd=x"), format!("?%FFpwd={MASK}"));
    }

    /// A text that ends inside a URL, a query key or a multi-byte character is returned whole.
    #[test]
    fn text_that_ends_mid_shape_is_untouched() {
        for text in [
            "why?",
            "a&b",
            "x?token",
            "x?token=",
            "a",
            "a:",
            "a://",
            "a://b",
            "a://b:c",
            "a://@",
            "é",
            "a://b:é",
            "ошибка —", // allow-cyrillic
        ] {
            let want = if text == "x?token=" {
                "x?token=***"
            } else {
                text
            };
            assert_eq!(redact_secrets(text), want, "{text:?}");
        }
    }

    /// A scheme may hold digits and `+ . -` after its first letter, and a secret query key may be of any length.
    #[test]
    fn a_compound_scheme_and_a_long_secret_key_are_redacted() {
        for (text, want) in [
            (
                "postgresql+psycopg2://u:S3cr3tPw@h/d",
                "postgresql+psycopg2://REDACTED@h/d",
            ),
            ("s3://key:S3cr3tPw@bucket", "s3://REDACTED@bucket"),
            ("x.y-z1://u:S3cr3tPw@h", "x.y-z1://REDACTED@h"),
            ("1a://u:S3cr3tPw@h", "1a://REDACTED@h"),
            ("12://u:S3cr3tPw@h", "12://u:S3cr3tPw@h"),
        ] {
            assert_eq!(redact_url_passwords(text), want, "{text:?}");
        }
        let key = format!("{}_token", "k".repeat(80));
        assert_eq!(
            redact_query_secrets(&format!("?{key}=S3cr3tPw&x=1")),
            format!("?{key}=***&x=1")
        );
    }

    const PASSWORD_KEYS: [&str; 12] = [
        "password",
        "PASSWORD",
        "sslpassword",
        "SSLPassword",
        "ssl_password",
        "db.password",
        "x-passwd",
        "passwd",
        "pwd",
        "Pwd",
        "MYSQL_PWD",
        "svc.admin.pwd",
    ];

    /// The URL query path masks the value of every key the keyword path calls a password, in any case.
    #[test]
    fn a_query_pair_whose_key_names_a_password_is_masked_in_any_case() {
        assert_eq!(
            redact_secrets(
                "postgres://u:p@h:5432/db?sslmode=require&sslpassword=TOPSECRET&SSLPassword=OTHER"
            ),
            "postgres://REDACTED@h:5432/db?sslmode=require&sslpassword=***&SSLPassword=***"
        );
        for key in PASSWORD_KEYS {
            for lead in ['?', '&'] {
                let text = format!("postgres://h/db{lead}a=1&{key}=S3cr3tPw&b=2");
                let want = format!("postgres://h/db{lead}a=1&{key}=***&b=2");
                assert_eq!(redact_secrets(&text), want, "{text:?}");
                assert_eq!(redact_query_secrets(&text), want, "{text:?}");
            }
        }
    }

    /// A query key spelled with percent escapes is read decoded: `pass%77ord` is `password`; a key that decodes to a non-secret stays.
    #[test]
    fn a_percent_encoded_query_key_that_names_a_password_is_masked() {
        for (text, want) in [
            (
                "postgres://h/db?pass%77ord=S3cr3tPw&b=2",
                "postgres://h/db?pass%77ord=***&b=2",
            ),
            (
                "postgres://h/db?a=1&%70wd=S3cr3tPw",
                "postgres://h/db?a=1&%70wd=***",
            ),
            (
                "postgres://h/db?ssl%50assword=S3cr3tPw",
                "postgres://h/db?ssl%50assword=***",
            ),
            (
                "postgres://h/db?options=-csearch_path%3Dp&application_name=x%20y",
                "postgres://h/db?options=-csearch_path%3Dp&application_name=x%20y",
            ),
        ] {
            assert_eq!(redact_secrets(text), want, "{text:?}");
        }
    }

    /// A query key whose percent escapes decode to non-ASCII text is read without a panic, and a password keyword after it is masked.
    #[test]
    fn a_query_key_decoding_to_non_ascii_text_is_read_without_a_panic() {
        for (text, want) in [
            ("?%FFn=x", "?%FFn=x"),
            ("&%C3%A9=x", "&%C3%A9=x"),
            ("?%E2%82%ACpwd=x", "?%E2%82%ACpwd=***"),
            ("?%C3%B1password=x", "?%C3%B1password=***"),
            ("?%FFpasswd=x", "?%FFpasswd=***"),
            ("?%FFa=x", "?%FFa=x"),
            ("?\u{FFFD}n=x", "?\u{FFFD}n=x"),
            ("\u{FFFD}n=x", "\u{FFFD}n=x"),
            ("\u{20AC}pwd=x", "\u{20AC}pwd=***"),
        ] {
            assert_eq!(redact_secrets(text), want, "{text:?}");
        }
    }

    /// One rule governs both paths: a key the keyword path masks is masked in a URL query, and a key it ignores stays.
    #[test]
    fn the_keyword_path_and_the_query_path_agree_on_which_keys_are_passwords() {
        for key in PASSWORD_KEYS {
            let keyword = redact_keyword_passwords(&format!("host=h {key}=S3cr3tPw dbname=d"));
            assert!(
                !keyword.contains("S3cr3tPw"),
                "keyword path leaked for {key}"
            );
            let query = redact_secrets(&format!("mysql://h/d?{key}=S3cr3tPw"));
            assert!(!query.contains("S3cr3tPw"), "query path leaked for {key}");
        }
        for key in [
            "sslmode",
            "passwordless",
            "pass",
            "application_name",
            "pwd2",
        ] {
            let keyword = format!("host=h {key}=visible dbname=d");
            assert_eq!(redact_keyword_passwords(&keyword), keyword, "{key}");
            let query = format!("mysql://h/d?{key}=visible");
            assert_eq!(redact_secrets(&query), query, "{key}");
        }
    }

    /// Query pairs that name no secret keep their text byte for byte.
    #[test]
    fn query_pairs_that_name_no_secret_are_left_intact() {
        for text in [
            "postgres://h/db?sslmode=require&application_name=svc&connect_timeout=10",
            "postgres://h/db?sslrootcert=/etc/ca.pem&sslcert=/etc/c.pem&sslkey=/etc/k.pem",
            "postgres://h/db?passwordless=1&user.name=bob&a.b.c=1",
            "https://acc.blob/x?sv=2020-08-04&se=2030&sp=r",
            "postgres://h/db?mypass=visible&keypwdx=visible",
        ] {
            assert_eq!(redact_secrets(text), text, "{text:?}");
        }
    }

    // ── redact_error ───────────────────────────────────────────────────────

    #[test]
    fn redact_error_strips_password_from_anyhow_chain() {
        let e = anyhow::anyhow!("connect failed to postgresql://alice:s3cret@db.prod/orders");
        let out = redact_error(&e);
        assert!(!out.contains("s3cret"));
        assert!(out.contains("REDACTED@db.prod"));
    }

    // ── F-NEW-C: multi-byte UTF-8 must round-trip ─────────────────────────────

    #[test]
    fn preserves_em_dash_and_other_multibyte_glyphs() {
        // Before the F-NEW-C fix, the byte-by-byte loop in
        // `redact_url_passwords` double-encoded every non-ASCII codepoint:
        // the em-dash `—` (UTF-8 e2 80 94) came out as `â\u{80}\u{94}`
        // (c3 a2 c2 80 c2 94).  This test pins the round-trip so the
        // regression cannot return silently in any error message.
        let s = "export 'orders': --resume refused — destination prefix has _SUCCESS";
        assert_eq!(
            redact_url_passwords(s),
            s,
            "non-URL text containing an em-dash must pass through unchanged"
        );

        let s2 = "сообщение об ошибке: cannot connect";
        assert_eq!(
            redact_url_passwords(s2),
            s2,
            "Cyrillic text must pass through unchanged"
        );

        // And it must still redact correctly when the string contains
        // BOTH multi-byte glyphs and a redactable URL.
        let s3 = "ошибка — postgresql://u:p@host/db: dropped";
        let out = redact_url_passwords(s3);
        assert!(out.contains("ошибка — postgresql://REDACTED@host/db"));
        assert!(!out.contains("u:p@"));
    }

    // ── SEC-RED: embedded `@` in password must not leak ───────────────────────

    #[test]
    fn sec_redact_url_password_with_at() {
        // SEC-RED V8: redact_url_passwords splits userinfo at the FIRST `@`,
        // leaking the password tail after an embedded `@`. The userinfo walk in
        // the scan breaks on the first `@` it sees, so the password
        // `p@ssw0rd` is split: only `p` is treated as the password and the
        // tail `ssw0rd` survives in the output. The userinfo terminator must be
        // the LAST `@` before the path/query (rfind semantics).
        let s = "connect failed to postgresql://rivet:p@ssw0rd@db.example.com:5432/orders";
        let out = redact_url_passwords(s);
        // No fragment of the password may survive. `ssw0rd` is the tail that
        // leaks today.
        assert!(
            !out.contains("ssw0rd"),
            "password tail after embedded @ must not leak: {out}"
        );
        assert!(
            !out.contains("p@ssw0rd"),
            "full password must not leak: {out}"
        );
        // Host and path must be retained, redacted to REDACTED@host.
        assert!(
            out.contains("postgresql://REDACTED@db.example.com:5432/orders"),
            "expected REDACTED@host with embedded-@ password stripped, got: {out}"
        );

        // Guard: a normal password (no embedded @) still redacts correctly so
        // this test pins the fix rather than just any change.
        let normal = "connect failed to postgresql://rivet:s3cret@db.example.com:5432/orders";
        let normal_out = redact_url_passwords(normal);
        assert!(
            !normal_out.contains("s3cret"),
            "normal password must still be redacted: {normal_out}"
        );
        assert!(
            normal_out.contains("postgresql://REDACTED@db.example.com:5432/orders"),
            "normal password redaction unchanged: {normal_out}"
        );
    }
}
