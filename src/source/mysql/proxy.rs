//! MySQL connection-proxy classifier — distinguishes direct connections from ProxySQL / MaxScale / generic multiplexers.

use mysql::Pool;
use mysql::prelude::*;

/// What the MySQL connection is actually talking to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MysqlProxyKind {
    /// Direct connection to a MySQL server — no proxy detected.
    Direct,
    /// ProxySQL: detected via either the `@@version_comment` signature or `@@proxy_version` (a ProxySQL-only system variable).
    ProxySql,
    /// MariaDB MaxScale: detected via `@@version_comment` containing "maxscale".
    MaxScale,
    /// An unknown transaction-mode multiplexer: detected.
    Multiplexed,
}

impl MysqlProxyKind {
    /// True for any non-direct connection (`is_proxy() == false` only for [`MysqlProxyKind::Direct`]).
    #[allow(dead_code)]
    pub fn is_proxy(self) -> bool {
        !matches!(self, MysqlProxyKind::Direct)
    }

    /// Stable label for the `executing query (connection=...)` debug log.
    pub fn log_label(self) -> &'static str {
        match self {
            MysqlProxyKind::Direct => "direct",
            MysqlProxyKind::ProxySql => "proxysql",
            MysqlProxyKind::MaxScale => "maxscale",
            MysqlProxyKind::Multiplexed => "proxy-multiplexed",
        }
    }

    /// One-time warning emitted at connect time.
    fn warn_message(self) -> Option<&'static str> {
        match self {
            MysqlProxyKind::Direct => None,
            MysqlProxyKind::ProxySql => Some(
                "MySQL proxy multiplexer detected (ProxySQL) — session variables \
                 set per-connection may not survive multiplexing; use direct connections \
                 for production exports",
            ),
            MysqlProxyKind::MaxScale => Some(
                "MySQL proxy detected (MaxScale) — queries may be rewritten or routed; \
                 verify SQL is accepted by the active MaxScale router (readwritesplit, \
                 readconnroute) and that session timeouts apply on the backend",
            ),
            MysqlProxyKind::Multiplexed => Some(
                "MySQL connection multiplexing detected (CONNECTION_ID() differs across \
                 queries) — session variables and temporary state may not persist across \
                 statements; use direct connections for production exports",
            ),
        }
    }
}

/// Pure classifier for proxy detection signals.
fn classify_mysql_proxy(
    proxysql_internal_accepted: bool,
    version_comment: Option<&str>,
    proxy_version: Option<&str>,
    connection_id_pair: Option<(u64, u64)>,
) -> MysqlProxyKind {
    if proxysql_internal_accepted {
        return MysqlProxyKind::ProxySql;
    }
    if let Some(v) = version_comment {
        let l = v.to_ascii_lowercase();
        if l.contains("proxysql") {
            return MysqlProxyKind::ProxySql;
        }
        if l.contains("maxscale") {
            return MysqlProxyKind::MaxScale;
        }
    }
    if proxy_version.is_some() {
        return MysqlProxyKind::ProxySql;
    }
    if let Some((a, b)) = connection_id_pair
        && a != b
    {
        return MysqlProxyKind::Multiplexed;
    }
    MysqlProxyKind::Direct
}

/// I/O wrapper around [`classify_mysql_proxy`].
pub(super) fn detect_mysql_proxy_kind(pool: &Pool) -> MysqlProxyKind {
    let mut conn = match pool.get_conn() {
        Ok(c) => c,
        Err(_) => return MysqlProxyKind::Direct,
    };
    detect_proxy_on_conn(&mut conn)
}

/// The probe + classify on an already-open connection.
pub(super) fn detect_proxy_on_conn<C: Queryable>(conn: &mut C) -> MysqlProxyKind {
    let proxysql_internal_accepted: bool = conn.query_drop("PROXYSQL INTERNAL SESSION").is_ok();
    let (version_comment, proxy_version) = proxy_globals(conn);
    let cid1: Option<u64> = conn.query_first("SELECT CONNECTION_ID()").unwrap_or(None);
    let cid2: Option<u64> = conn.query_first("SELECT CONNECTION_ID()").unwrap_or(None);
    let pair = match (cid1, cid2) {
        (Some(a), Some(b)) => Some((a, b)),
        _ => None,
    };
    classify_mysql_proxy(
        proxysql_internal_accepted,
        version_comment.as_deref(),
        proxy_version.as_deref(),
        pair,
    )
}

/// `version_comment` and `proxy_version` in one `SHOW GLOBAL VARIABLES` by name (no `@@`, no `LIKE` scan).
fn proxy_globals<C: Queryable>(conn: &mut C) -> (Option<String>, Option<String>) {
    let rows: Vec<(String, String)> = conn
        .query("SHOW GLOBAL VARIABLES WHERE Variable_name IN ('version_comment', 'proxy_version')")
        .unwrap_or_default();
    let value = |name: &str| {
        rows.iter()
            .find(|(n, _)| n.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.clone())
    };
    (value("version_comment"), value("proxy_version"))
}

/// The proxy kind of `url`, probed once per process on `pool`'s own connection: no dial of its own.
pub(super) fn proxy_kind_of(url: &str, pool: &Pool) -> MysqlProxyKind {
    static KNOWN: std::sync::OnceLock<
        std::sync::Mutex<std::collections::HashMap<String, MysqlProxyKind>>,
    > = std::sync::OnceLock::new();
    let known = KNOWN.get_or_init(Default::default);
    if let Some(kind) = known.lock().unwrap_or_else(|e| e.into_inner()).get(url) {
        return *kind;
    }
    let kind = detect_mysql_proxy_kind(pool);
    known
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .insert(url.to_string(), kind);
    kind
}

/// Emit the one-time connect-time warning for a non-direct proxy kind.
pub(super) fn warn_proxy_kind(kind: MysqlProxyKind) {
    if let Some(msg) = kind.warn_message() {
        log::warn!("{msg}");
    }
}

#[cfg(test)]
mod tests {
    use super::{MysqlProxyKind, classify_mysql_proxy};
    use mysql::prelude::Queryable;

    #[test]
    fn proxy_classify_direct_when_no_signals() {
        let kind = classify_mysql_proxy(
            false,
            Some("MySQL Community Server - GPL"),
            None,
            Some((42, 42)),
        );
        assert_eq!(kind, MysqlProxyKind::Direct);
    }

    #[test]
    fn proxy_classify_direct_when_all_signals_missing() {
        let kind = classify_mysql_proxy(false, None, None, None);
        assert_eq!(kind, MysqlProxyKind::Direct);
    }

    #[test]
    fn proxy_classify_proxysql_via_internal_command() {
        let kind = classify_mysql_proxy(
            true,
            Some("MySQL Community Server - GPL"),
            None,
            Some((7, 7)),
        );
        assert_eq!(kind, MysqlProxyKind::ProxySql);
    }

    #[test]
    fn proxy_classify_proxysql_via_banner() {
        let kind = classify_mysql_proxy(
            false,
            Some("(ProxySQL) High Performance MySQL Proxy"),
            None,
            None,
        );
        assert_eq!(kind, MysqlProxyKind::ProxySql);
    }

    #[test]
    fn proxy_classify_proxysql_via_banner_lowercase() {
        let kind = classify_mysql_proxy(false, Some("(proxysql) hpmp"), None, None);
        assert_eq!(kind, MysqlProxyKind::ProxySql);
    }

    #[test]
    fn proxy_classify_proxysql_via_proxy_version_only() {
        let kind = classify_mysql_proxy(
            false,
            Some("MySQL Community Server - GPL"),
            Some("2.5.5-percona-1.1"),
            Some((1, 1)),
        );
        assert_eq!(kind, MysqlProxyKind::ProxySql);
    }

    #[test]
    fn proxy_classify_maxscale_via_banner() {
        let kind = classify_mysql_proxy(
            false,
            Some("MariaDB MaxScale 22.08.4-ge6a8d35ec source distribution"),
            None,
            Some((1, 1)),
        );
        assert_eq!(kind, MysqlProxyKind::MaxScale);
    }

    #[test]
    fn proxy_classify_proxysql_internal_takes_precedence_over_banner_maxscale() {
        let kind = classify_mysql_proxy(
            true,
            Some("MariaDB MaxScale 22.08.4 source distribution"),
            None,
            None,
        );
        assert_eq!(kind, MysqlProxyKind::ProxySql);
    }

    #[test]
    fn proxy_classify_proxysql_takes_precedence_over_maxscale_in_banner() {
        let kind = classify_mysql_proxy(
            false,
            Some("ProxySQL bridging MaxScale upstream"),
            None,
            None,
        );
        assert_eq!(kind, MysqlProxyKind::ProxySql);
    }

    #[test]
    fn proxy_classify_multiplexed_via_connection_id_drift() {
        let kind = classify_mysql_proxy(
            false,
            Some("MySQL Community Server"),
            None,
            Some((100, 200)),
        );
        assert_eq!(kind, MysqlProxyKind::Multiplexed);
    }

    #[test]
    fn proxy_classify_direct_when_connection_id_pair_missing() {
        let kind = classify_mysql_proxy(false, Some("MySQL Community Server"), None, None);
        assert_eq!(kind, MysqlProxyKind::Direct);
    }

    #[test]
    fn proxy_classify_direct_when_connection_ids_match() {
        let kind = classify_mysql_proxy(false, Some("MySQL Community Server"), None, Some((7, 7)));
        assert_eq!(kind, MysqlProxyKind::Direct);
    }

    #[test]
    fn proxy_classify_proxysql_pool_size_one_still_caught_by_banner() {
        let kind = classify_mysql_proxy(false, Some("(ProxySQL) HPMP"), None, Some((5, 5)));
        assert_eq!(kind, MysqlProxyKind::ProxySql);
    }

    #[test]
    fn proxy_classify_proxysql_default_config_still_detected_via_internal() {
        let kind = classify_mysql_proxy(
            true,
            Some("MySQL Community Server - GPL"),
            None,
            Some((42, 42)),
        );
        assert_eq!(
            kind,
            MysqlProxyKind::ProxySql,
            "default-config ProxySQL must be detectable via PROXYSQL INTERNAL signal alone"
        );
    }

    #[test]
    fn proxy_kind_is_proxy_helper_matches_variants() {
        assert!(!MysqlProxyKind::Direct.is_proxy());
        assert!(MysqlProxyKind::ProxySql.is_proxy());
        assert!(MysqlProxyKind::MaxScale.is_proxy());
        assert!(MysqlProxyKind::Multiplexed.is_proxy());
    }

    #[test]
    fn proxy_kind_direct_has_no_warning() {
        assert!(MysqlProxyKind::Direct.warn_message().is_none());
    }

    #[test]
    fn proxy_kind_non_direct_variants_have_warnings() {
        for k in [
            MysqlProxyKind::ProxySql,
            MysqlProxyKind::MaxScale,
            MysqlProxyKind::Multiplexed,
        ] {
            assert!(
                k.warn_message().is_some(),
                "{k:?} must emit a warning at connect time"
            );
        }
    }

    #[test]
    fn proxy_kind_log_labels_are_stable_and_distinct() {
        let labels = [
            MysqlProxyKind::Direct.log_label(),
            MysqlProxyKind::ProxySql.log_label(),
            MysqlProxyKind::MaxScale.log_label(),
            MysqlProxyKind::Multiplexed.log_label(),
        ];
        assert_eq!(
            labels,
            ["direct", "proxysql", "maxscale", "proxy-multiplexed"]
        );
    }

    /// The probe reads its two global variables in one indexed statement: two `LIKE` scans cost ~1270 rows (#694).
    #[test]
    #[ignore = "live: requires docker compose mysql"]
    fn the_probe_reads_its_globals_without_a_full_variables_scan() {
        let mut c = mysql::Conn::new(
            mysql::Opts::from_url("mysql://rivet:rivet@127.0.0.1:3306/rivet").unwrap(),
        )
        .unwrap();
        let scanned = |c: &mut mysql::Conn| -> u64 {
            c.query_first::<(String, u64), _>("SHOW SESSION STATUS LIKE 'Handler_read_rnd_next'")
                .unwrap()
                .unwrap()
                .1
        };
        let before = scanned(&mut c);
        assert_eq!(super::detect_proxy_on_conn(&mut c), MysqlProxyKind::Direct);
        let rows = scanned(&mut c) - before;
        assert!(rows < 50, "the proxy probe scanned {rows} rows");
        let (comment, proxy) = super::proxy_globals(&mut c);
        assert!(comment.is_some_and(|v| v.contains("MySQL")) && proxy.is_none());
    }
}
