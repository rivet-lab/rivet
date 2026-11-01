//! Pure readers of the text of a connection URL: its authority, the userinfo a credential mask cuts, and the host list.

/// The authority of a connection URL: its userinfo, its host list, and the text from the first `/`, `?` or `#` on.
pub(crate) struct Authority<'a> {
    pub userinfo: Option<&'a str>,
    pub hosts: &'a str,
    pub rest: &'a str,
}

/// The RFC 3986 authority of a `scheme://…` URL: it ends at the first `/`, `?` or `#`, and the userinfo is what precedes the last `@` inside it; `None` without `://`.
pub(crate) fn authority(url: &str) -> Option<Authority<'_>> {
    url.split_once("://")
        .map(|(_, after)| split_authority(after))
}

/// [`authority`] of the text after the scheme.
fn split_authority(after: &str) -> Authority<'_> {
    let (authority, rest) = after.split_at(after.find(['/', '?', '#']).unwrap_or(after.len()));
    match authority.rsplit_once('@') {
        Some((userinfo, hosts)) => Authority {
            userinfo: Some(userinfo),
            hosts,
            rest,
        },
        None => Authority {
            userinfo: None,
            hosts: authority,
            rest,
        },
    }
}

/// The `host[:port][,host:port…]` span of a URL, scheme and userinfo removed; the whole text is the authority when it has no scheme.
pub(crate) fn host_port_span(url: &str) -> &str {
    authority(url).unwrap_or_else(|| split_authority(url)).hosts
}

/// Whether `hosts` reads as a host list: every entry is a name or a bracketed address with an optional numeric port.
fn is_host_list(hosts: &str) -> bool {
    hosts.split(',').all(|entry| {
        let name = match entry.strip_prefix('[').and_then(|r| r.split_once(']')) {
            Some((_, tail)) => {
                (tail.is_empty() || tail.strip_prefix(':').is_some_and(is_port)).then_some("")
            }
            None => match entry.rsplit_once(':') {
                Some((name, port)) => is_port(port).then_some(name),
                None => Some(entry),
            },
        };
        name.is_some_and(|n| !n.contains([':', ';', '=', '@']) && !n.contains(char::is_whitespace))
    })
}

/// Whether `port` is empty or all digits.
fn is_port(port: &str) -> bool {
    port.bytes().all(|b| b.is_ascii_digit())
}

/// Which userinfo a credential scrubber masks.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Mask {
    Password,
    AnyUserinfo,
}

/// Where a URL's credentials sit in `text`, as (userinfo start, the `@` ending it): the LAST `@` ends it, since a raw password may hold `/ ? # @ :`.
pub(crate) fn credential_span(text: &str, mask: Mask) -> Option<(usize, usize)> {
    match mask {
        Mask::Password => password_span(text),
        Mask::AnyUserinfo => any_userinfo_span(text),
    }
}

/// The userinfo of the first `scheme://user:password@` in a whitespace-free word, as (userinfo start, its `@`): the LAST `@` of the word ends it, and a `:` must precede that `@`.
fn password_span(word: &str) -> Option<(usize, usize)> {
    let at = word.rfind('@')?;
    let last_colon = word.get(..at)?.rfind(':')?;
    word.match_indices("://")
        .map(|(scheme_end, _)| (scheme_end, scheme_end + 3))
        .take_while(|(_, userinfo)| *userinfo <= last_colon)
        .find(|(scheme_end, _)| {
            word.as_bytes()[..*scheme_end]
                .iter()
                .rev()
                .take_while(|b| is_scheme_byte(**b))
                .any(u8::is_ascii_alphabetic)
        })
        .map(|(_, userinfo)| (userinfo, at))
}

/// A byte a URL scheme may hold after its first letter.
fn is_scheme_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || matches!(b, b'+' | b'.' | b'-')
}

/// The userinfo of a `scheme://` URL as (userinfo start, its `@`): the LAST `@` ends it when it sits in the authority or a `:` precedes it.
fn any_userinfo_span(raw: &str) -> Option<(usize, usize)> {
    let scheme = raw.find("://")? + 3;
    let rest = &raw[scheme..];
    let at = rest.rfind('@')?;
    let before = &rest[..at];
    (!before.contains(['/', '?', '#']) || before.contains(':')).then_some((scheme, scheme + at))
}

/// The text of a URL after its userinfo (the host list and path, query included): the RFC 3986 authority decides it, and a host list that is no host list falls back to the credential reading.
pub(crate) fn server_and_path(url: &str) -> &str {
    let Some(a) = authority(url) else {
        return url;
    };
    let strict_ok = a.userinfo.is_some() || is_host_list(a.hosts);
    let rest_start = url.len() - a.rest.len();
    if strict_ok {
        let hosts_start = rest_start - a.hosts.len();
        return &url[hosts_start..];
    }
    match any_userinfo_span(url) {
        Some((_, at)) => &url[at + 1..],
        None => &url[url.find("://").map_or(0, |i| i + 3)..],
    }
}

#[cfg(test)]
pub(crate) mod corpus {
    /// Every `scheme://userinfo host tail` shape: each userinfo form, host list, path and query/fragment with an `@`.
    pub(crate) fn urls(scheme: &str) -> Vec<String> {
        let userinfos = [
            "", "u@", "u:@", "u:pw@", ":pw@", "u:p%40w@", "u:p@w@", "u:p/w@", "u:p?w@", "u:p#w@",
            "u:a:b@",
        ];
        let hosts = ["h1", "h1:5432", "[::1]:5432", "h1:1,h2:2", "h1:"];
        let tails = [
            "",
            "/db",
            "/db/",
            "/db?sslmode=require",
            "/db?application_name=svc@corp",
            "/db?x=a:b@c",
            "/db#frag@x",
            "?k=v@corp",
            "#f@x",
        ];
        let mut out = Vec::new();
        for userinfo in userinfos {
            for host in hosts {
                for tail in tails {
                    out.push(format!("{scheme}://{userinfo}{host}{tail}"));
                }
            }
        }
        out
    }

    /// The schemes of the engines rivet reads.
    pub(crate) const SOURCES: [(crate::config::SourceType, &str); 7] = {
        use crate::config::SourceType::*;
        [
            (Postgres, "postgresql"),
            (Postgres, "postgres"),
            (Mysql, "mysql"),
            (Mssql, "sqlserver"),
            (Oracle, "oracle"),
            (Mongo, "mongodb"),
            (Mongo, "mongodb+srv"),
        ]
    };

    /// The userinfo-free reading of `url` the way main read it before the authority seam: the last `@` of the text past `://` ended the userinfo when it sat in the authority or a `:` preceded it.
    pub(crate) fn legacy_after_userinfo(url: &str) -> &str {
        let Some(scheme) = url.find("://").map(|i| i + 3) else {
            return url;
        };
        let rest = &url[scheme..];
        let authority_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
        match rest.rfind('@') {
            Some(at) if at < authority_end || rest[..at].contains(':') => &url[scheme + at + 1..],
            _ => rest,
        }
    }

    /// Whether `url` holds an `@` after its authority, the shape main read as a userinfo end.
    pub(crate) fn at_after_authority(url: &str) -> bool {
        super::authority(url).is_some_and(|a| a.rest.contains('@'))
    }
}

#[cfg(test)]
mod authority_tests {
    use super::corpus::{SOURCES, at_after_authority, legacy_after_userinfo, urls};
    use super::*;
    use crate::config::source_state_key;

    fn legacy_key(engine: crate::config::SourceType, url: &str) -> String {
        let url = crate::redact::redact_keyword_passwords(url);
        let rest = legacy_after_userinfo(&url);
        let rest = rest.split_once("://").map_or(rest, |_| rest);
        let at_host = rest.split(['?', '#']).next().unwrap_or(rest);
        let key = format!("{engine:?}://{}", at_host.trim_end_matches('/')).to_lowercase();
        crate::config::canonical_source_key(&key)
    }

    #[test]
    fn the_authority_ends_at_the_first_slash_question_mark_or_hash() {
        let a = authority("postgresql://u:p@h1:5432/db?x=a@b").unwrap();
        assert_eq!(
            (a.userinfo, a.hosts, a.rest),
            (Some("u:p"), "h1:5432", "/db?x=a@b")
        );
        let a = authority("postgresql://h1:5432/a?application_name=svc@corp").unwrap();
        assert_eq!((a.userinfo, a.hosts), (None, "h1:5432"));
        let a = authority("mysql://u:p@w@h#f@x").unwrap();
        assert_eq!((a.userinfo, a.hosts, a.rest), (Some("u:p@w"), "h", "#f@x"));
        let a = authority("mongodb://a:1,b:2?k=v@c").unwrap();
        assert_eq!((a.userinfo, a.hosts), (None, "a:1,b:2"));
        assert!(authority("no scheme at all@x").is_none());
        assert_eq!(host_port_span("h1:5432/db?x@y"), "h1:5432");
    }

    #[test]
    fn a_host_list_is_names_with_numeric_ports() {
        for ok in ["h", "h:5432", "h:", "[::1]", "[::1]:5432", "a:1,b,c:3", ""] {
            assert!(is_host_list(ok), "{ok:?}");
        }
        for bad in [
            "u:pa", "u:pw:x", "a;b=c", "h 1", "[::1]x", "[::1]:x", "a:1,u:pa", "u@h",
        ] {
            assert!(!is_host_list(bad), "{bad:?}");
        }
    }

    #[test]
    fn a_state_key_is_the_legacy_key_wherever_no_at_follows_the_authority() {
        let mut same = 0;
        for (engine, scheme) in SOURCES {
            for url in urls(scheme) {
                let (new, old) = (source_state_key(engine, &url), legacy_key(engine, &url));
                if at_after_authority(&url) {
                    continue;
                }
                assert_eq!(new, old, "{url}");
                same += 1;
            }
        }
        assert!(same > 1000, "{same}");
    }

    #[test]
    fn a_state_key_changes_only_where_an_at_follows_a_well_formed_authority() {
        let mut changed = 0;
        for (engine, scheme) in SOURCES {
            for url in urls(scheme) {
                let (new, old) = (source_state_key(engine, &url), legacy_key(engine, &url));
                if new == old {
                    continue;
                }
                let a = authority(&url).unwrap();
                assert!(
                    a.rest.contains('@') && (a.userinfo.is_some() || is_host_list(a.hosts)),
                    "{url}: {old} -> {new}"
                );
                assert!(!new.contains("corp") && !new.contains("@"), "{url}: {new}");
                changed += 1;
            }
        }
        assert!(changed > 200, "{changed}");
    }

    #[test]
    fn two_hosts_never_share_a_state_key() {
        for (engine, scheme) in SOURCES {
            for url in urls(scheme).iter().filter(|u| {
                let a = authority(u).unwrap();
                u.contains("h1") && (a.userinfo.is_some() || is_host_list(a.hosts))
            }) {
                let other = url.replace("h1", "h9");
                assert_ne!(
                    source_state_key(engine, url),
                    source_state_key(engine, &other),
                    "{url}"
                );
            }
        }
    }

    #[test]
    fn a_raw_slash_password_stays_out_of_the_state_key() {
        for url in [
            "postgresql://u:pa/ss@h/db",
            "postgresql://u:pa?ss@h/db",
            "mysql://u:pa#ss@h:3306/db",
            "sqlserver://sa:Qm4/Tz8@h:1433/db",
        ] {
            let key = source_state_key(crate::config::SourceType::Postgres, url);
            assert!(
                !key.contains("ss") && !key.contains("Tz8") && !key.contains('@'),
                "{key}"
            );
        }
    }

    #[test]
    fn the_credential_span_masks_the_last_at_of_the_text() {
        let any = |u| credential_span(u, Mask::AnyUserinfo);
        let pw = |u| credential_span(u, Mask::Password);
        assert_eq!(any("postgresql://user:pass@host/db"), Some((13, 22)));
        assert_eq!(any("postgresql://user@host/db"), Some((13, 17)));
        assert_eq!(pw("postgresql://user@host/db"), None);
        assert_eq!(pw("postgresql://user:pass@host/db"), Some((13, 22)));
        assert_eq!(any("postgresql://host/db"), None);
        assert_eq!(any("postgresql://host/db?filter=a@b"), None);
        assert_eq!(any("postgresql://u:p/w@host/db"), Some((13, 18)));
        assert_eq!(pw("postgresql://u:p@w@host"), Some((13, 18)));
        assert_eq!(pw("postgresql://u:p@h"), Some((13, 16)));
    }

    #[test]
    fn server_and_path_reads_the_authority_and_falls_back_for_a_non_host_list() {
        assert_eq!(server_and_path("pg://u:p@h:5/db?x@y"), "h:5/db?x@y");
        assert_eq!(
            server_and_path("pg://h1:5/a?x=svc@corp"),
            "h1:5/a?x=svc@corp"
        );
        assert_eq!(server_and_path("pg://u:pa/ss@h/db"), "h/db");
        assert_eq!(server_and_path("pg://u:pa/ss/db"), "u:pa/ss/db");
        assert_eq!(server_and_path("no scheme"), "no scheme");
    }
}
