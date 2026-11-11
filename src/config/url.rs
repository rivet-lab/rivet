//! Pure readers of a connection URL: whether it stays on the box and its `sslmode`.

use super::TlsConfig;
use crate::error::Result;
use crate::url_text::host_port_span;

/// Whether every host of a `scheme://[user[:pass]@]host[:port][/db][?…]` URL is loopback; fails closed on any URL it cannot confidently read.
pub(crate) fn host_is_loopback(url: &str) -> bool {
    let host_port = host_port_span(url);
    // A comma seedlist (`host1:p1,host2:p2` — valid for MongoDB AND multi-host
    // PostgreSQL) is loopback ONLY if EVERY host is: reading just the first host
    // let `127.0.0.1:5432,evil.com:5432` dial evil.com in plaintext under the
    // gate (bug-hunt find). Empty authority ⇒ not loopback (fail closed).
    !host_port.is_empty()
        && host_port.split(',').all(one_host_is_loopback)
        && query_hosts(url).all(query_host_is_local)
}

/// The libpq `host=` / `hostaddr=` query values a driver dials besides the authority.
fn query_hosts(url: &str) -> impl Iterator<Item = &str> {
    let query = url.split_once('?').map_or("", |(_, q)| q);
    let query = query.split('#').next().unwrap_or(query);
    query
        .split('&')
        .filter_map(|pair| pair.split_once('='))
        .filter(|(k, _)| matches!(*k, "host" | "hostaddr"))
        .flat_map(|(_, v)| v.split(','))
}

/// A query host that stays on the box: a unix-socket path, or a loopback name/address.
fn query_host_is_local(v: &str) -> bool {
    v.starts_with('/')
        || v.get(..3).is_some_and(|p| p.eq_ignore_ascii_case("%2f"))
        || v.parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
        || one_host_is_loopback(v)
}

/// Loopback test for a single `host[:port]` (or bracketed `[ipv6][:port]`).
fn one_host_is_loopback(host_port: &str) -> bool {
    // IPv6 literals are bracketed (`[::1]:5432`); the host is the bracketed span,
    // and any `:` inside is part of the address.
    let host = if let Some(rest) = host_port.strip_prefix('[') {
        match rest.split_once(']') {
            Some((h, _)) => h,
            None => return false, // unterminated bracket — fail closed
        }
    } else {
        // Bare host or IPv4: the host ends at the (single) port `:`.
        host_port.split(':').next().unwrap_or(host_port)
    };

    if host.eq_ignore_ascii_case("localhost") {
        return true;
    }
    // `IpAddr::is_loopback` covers the whole 127.0.0.0/8 block and `::1`.
    host.parse::<std::net::IpAddr>()
        .is_ok_and(|ip| ip.is_loopback())
}

/// The URL without its `sslmode`, and the [`TlsConfig`] that `sslmode` asks for:
/// `require` / `verify-ca` / `verify-full` enforce, anything else is `None`; last occurrence wins, like libpq.
pub(crate) fn url_tls(url: &str) -> (String, Option<TlsConfig>) {
    let query = url.split_once('?').map_or("", |(_, q)| q);
    let mut mode = None;
    for pair in query.split('&') {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        if key != "sslmode" {
            continue;
        }
        mode = match value {
            "require" => Some(crate::config::TlsMode::Require),
            "verify-ca" => Some(crate::config::TlsMode::VerifyCa),
            "verify-full" => Some(crate::config::TlsMode::VerifyFull),
            _ => None,
        };
    }
    let tls = mode.map(|mode| TlsConfig {
        mode,
        ..TlsConfig::default()
    });
    (strip_url_query_key(url, "sslmode"), tls)
}

/// Remove a query parameter (case-insensitive key) from a URL, keeping the rest of the query.
pub(crate) fn strip_url_query_key(url: &str, key: &str) -> String {
    let Some((base, query)) = url.split_once('?') else {
        return url.to_string();
    };
    let kept: Vec<&str> = query
        .split('&')
        .filter(|pair| {
            let k = pair.split('=').next().unwrap_or(pair);
            !k.eq_ignore_ascii_case(key)
        })
        .collect();
    if kept.is_empty() {
        base.to_string()
    } else {
        format!("{base}?{}", kept.join("&"))
    }
}

/// Refuse an `http://` URL on a remote host before any request carries `carries` through it in cleartext.
pub(crate) fn require_https_or_loopback(
    url: &str,
    sink: &str,
    carries: &str,
    remedy: &str,
) -> Result<()> {
    let cleartext = url
        .trim_start()
        .get(..7)
        .is_some_and(|scheme| scheme.eq_ignore_ascii_case("http://"));
    if cleartext && !host_is_loopback(url) {
        let host = host_port_span(url);
        crate::rivet_bail!(
            crate::error::codes::CONFIG_REMOTE_PLAINTEXT,
            "{sink}: refusing to send {carries} to the remote (non-loopback) host `{host}` over \
             cleartext http://; they would cross the network unencrypted. {remedy}"
        );
    }
    Ok(())
}

/// Refuse a remote `http://` ClickHouse `load.url`, which carries the loader password and every row, unless `allow_plaintext`.
pub(crate) fn require_https_clickhouse_url(url: &str, allow_plaintext: bool) -> Result<()> {
    if allow_plaintext {
        return Ok(());
    }
    require_https_or_loopback(
        url,
        "ClickHouse load `url`",
        "the loader password and every loaded row",
        "Use an `https://` URL (with `load.ca_file:` for a private CA), or set \
         `load.allow_plaintext: true` if this network path is already trusted.",
    )
}

/// Refuse a Slack webhook that is `http://` on a remote host: the URL is the posting secret.
pub(crate) fn require_https_webhook(url: &str) -> Result<()> {
    require_https_or_loopback(
        url,
        "Slack notification `webhook_url`",
        "the webhook secret and the run summary",
        "Use the `https://` webhook URL Slack gave you.",
    )
}

/// Refuse a remote `http://` BigQuery API endpoint override, which carries the OAuth bearer token and every query.
pub(crate) fn require_https_bigquery_endpoint(url: &str) -> Result<()> {
    require_https_or_loopback(
        url,
        "BigQuery `RIVET_BQ_API_ENDPOINT`",
        "the OAuth bearer token and every query",
        "Unset `RIVET_BQ_API_ENDPOINT`, or point it at an `https://` or loopback endpoint.",
    )
}

#[cfg(test)]
mod url_tls_tests {
    use super::url_tls;
    use crate::config::TlsMode;

    fn mode(url: &str) -> Option<TlsMode> {
        url_tls(url).1.map(|t| t.mode)
    }

    #[test]
    fn sslmode_enforced_values_map_to_tls_modes() {
        assert_eq!(
            mode("postgresql://u:p@h:5432/d?sslmode=require"),
            Some(TlsMode::Require)
        );
        assert_eq!(
            mode("postgresql://u:p@h/d?sslmode=verify-ca"),
            Some(TlsMode::VerifyCa)
        );
        assert_eq!(
            mode("mysql://u:p@h/d?sslmode=verify-full"),
            Some(TlsMode::VerifyFull)
        );
    }

    /// `require`, not the `#[default]` VerifyFull, so dropping `mode` from the struct is visible.
    #[test]
    fn url_sslmode_lands_in_the_config_it_builds() {
        let t = url_tls("postgresql://u:p@h/db?sslmode=require")
            .1
            .expect("require maps");
        assert_eq!(t.mode, TlsMode::Require);
        assert!(!t.accept_invalid_certs && t.ca_file.is_none());
    }

    #[test]
    fn sslmode_plaintext_missing_and_unrecognized_values_stay_plaintext() {
        for url in [
            "postgresql://u:p@localhost/d",
            "postgresql://u:p@db/d?sslmode=disable",
            "postgresql://u:p@db/d?sslmode=prefer",
            "postgresql://u:p@db/d?sslmode=allow",
            "postgresql://u:p@db/d?sslmode=REQUIRE",
            "postgresql://u:p@db/d?sslmode=garbage",
            "postgresql://u:p@db/d?sslmode",
            "postgresql://u:p@db/d?sslmode=",
        ] {
            assert_eq!(mode(url), None, "url: {url}");
        }
    }

    #[test]
    fn sslmode_exact_key_among_other_params_and_last_occurrence_wins() {
        assert_eq!(mode("postgresql://u:p@db/d?xsslmode=require"), None);
        assert_eq!(
            mode("postgresql://u:p@db/d?connect_timeout=10&sslmode=require&application_name=x"),
            Some(TlsMode::Require)
        );
        assert_eq!(
            mode("postgresql://u:p@db/d?sslmode=disable&sslmode=require"),
            Some(TlsMode::Require)
        );
        assert_eq!(
            mode("postgresql://u:p@db/d?sslmode=require&sslmode=disable"),
            None
        );
    }

    #[test]
    fn url_tls_strips_sslmode_and_keeps_other_params() {
        assert_eq!(
            url_tls("mysql://u:p@db:3306/d?stmt_cache_size=5&sslmode=require&prefer_socket=false")
                .0,
            "mysql://u:p@db:3306/d?stmt_cache_size=5&prefer_socket=false"
        );
        assert_eq!(
            url_tls("mysql://u:p@db/d?sslmode=require").0,
            "mysql://u:p@db/d"
        );
        assert_eq!(url_tls("mysql://u:p@db/d").0, "mysql://u:p@db/d");
    }

    /// The MySQL driver refuses any URL parameter it does not know, so `sslmode` must be gone before it parses.
    #[test]
    fn mysql_url_with_sslmode_parses_after_url_tls() {
        let raw = "mysql://u:p@db.prod:3306/d?sslmode=verify-full&stmt_cache_size=5";
        assert!(
            mysql::Opts::from_url(raw).is_err(),
            "the driver must reject the raw URL"
        );
        let (clean, tls) = url_tls(raw);
        assert!(
            mysql::Opts::from_url(&clean).is_ok(),
            "cleaned URL must parse: {clean}"
        );
        assert_eq!(tls.map(|t| t.mode), Some(TlsMode::VerifyFull));
    }
}

#[cfg(test)]
mod loopback_tests {
    use super::{host_is_loopback, host_port_span};

    #[test]
    fn loopback_variants_are_loopback() {
        assert!(host_is_loopback(
            "postgresql://rivet:rivet@127.0.0.1:5432/rivet"
        ));
        assert!(host_is_loopback(
            "postgresql://rivet:rivet@localhost:5432/rivet"
        ));
        assert!(host_is_loopback("mysql://root@127.0.0.1:3306/db"));
        // Whole 127.0.0.0/8 block is loopback.
        assert!(host_is_loopback("postgresql://u:p@127.255.0.9/db"));
        // IPv6 loopback, bracketed with and without a port.
        assert!(host_is_loopback("postgresql://u:p@[::1]:5432/db"));
        assert!(host_is_loopback("sqlserver://sa:pw@[::1]/master"));
        // Case-insensitive host, no port, no db.
        assert!(host_is_loopback("mysql://root@LOCALHOST"));
        // An `@` inside the password must not be mistaken for the host boundary.
        assert!(host_is_loopback("postgresql://u:p@ss@127.0.0.1:5432/db"));
    }

    #[test]
    fn roast_seedlist_with_any_remote_host_is_not_loopback() {
        // Multi-host / seedlist authority (`host1:p1,host2:p2`): the TLS gate must
        // treat it as loopback ONLY if EVERY host is loopback. Reading just the
        // first host let `127.0.0.1:5432,evil.com:5432` (a valid PostgreSQL and
        // MongoDB seedlist) pass the gate and dial evil.com in plaintext
        // (bug-hunt find; the shared gate reaches every engine, PG supports
        // multi-host URLs).
        assert!(!host_is_loopback(
            "postgresql://u:p@127.0.0.1:5432,evil.com:5432/db"
        ));
        assert!(!host_is_loopback(
            "mongodb://u:p@127.0.0.1:27017,evil.com:27017/db"
        ));
        // All-loopback seedlist stays loopback.
        assert!(host_is_loopback(
            "mongodb://u:p@127.0.0.1:27017,[::1]:27018/db"
        ));
    }

    #[test]
    fn remote_hosts_are_not_loopback() {
        assert!(!host_is_loopback(
            "postgresql://rivet:rivet@10.255.255.1:5432/rivet"
        ));
        assert!(!host_is_loopback(
            "postgresql://u:p@db.example.com:5432/app"
        ));
        assert!(!host_is_loopback("mysql://root@192.168.1.10:3306/db"));
        assert!(!host_is_loopback("sqlserver://sa:pw@10.0.0.5:1433/master"));
        // Not loopback: an unbracketed IPv6-looking address won't parse here, so
        // it fails closed (treated as remote).
        assert!(!host_is_loopback("postgresql://u:p@::1:5432/db"));
    }

    #[test]
    fn host_port_span_extracts_the_authority() {
        assert_eq!(host_port_span("mysql://u:p@host:3306/db"), "host:3306");
        assert_eq!(
            host_port_span("postgres://127.0.0.1:5432/db"),
            "127.0.0.1:5432"
        );
        // No authority at all.
        assert_eq!(host_port_span("mysql://"), "");
        assert_eq!(host_port_span("postgres:///db"), "");
    }
}

#[cfg(test)]
mod plaintext_gate_tests {
    use super::require_https_or_loopback;

    fn gate(url: &str) -> crate::error::Result<()> {
        require_https_or_loopback(url, "sink", "secrets", "Use https.")
    }

    #[test]
    fn a_remote_http_url_is_refused_by_code_naming_the_host_and_the_remedy() {
        for url in [
            "http://ch.example.com:8123",
            "HTTP://ch.example.com",
            "  http://10.1.2.3/x",
            "http://user:pw@ch.example.com:8123/db",
            "http://127.0.0.1:1,ch.example.com:2",
        ] {
            let e = gate(url).unwrap_err();
            assert_eq!(
                crate::error::error_code(&e),
                Some("RIVET_CONFIG_REMOTE_PLAINTEXT"),
                "{url}"
            );
            let text = format!("{e:#}");
            assert!(text.starts_with("sink: refusing to send secrets"), "{text}");
            assert!(text.ends_with("Use https."), "{text}");
            assert!(!text.contains("pw"), "{text}");
        }
    }

    #[test]
    fn https_loopback_and_unreadable_urls_pass() {
        for url in [
            "https://ch.example.com",
            "HTTPS://ch.example.com",
            "http://localhost:8123",
            "http://127.0.0.1:9",
            "http://[::1]:9/x",
            "ch.example.com:8123",
            "",
            "ht",
            "http:/é",
        ] {
            gate(url).unwrap_or_else(|e| panic!("{url}: {e:#}"));
        }
    }
}
