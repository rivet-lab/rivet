//! A Snowflake account identifier, read once at config load and again by the SQL API client.

const SNOWFLAKE_HOST_SUFFIX: &str = ".snowflakecomputing.com";

/// A Snowflake account identifier, as the host name and as the JWT claims name it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Account {
    pub(crate) host: String,
    pub(crate) jwt_name: String,
}

impl Account {
    /// Read an account identifier: an organisation-account name (`myorg-myaccount`) or an
    /// account locator, with its region when it has one (`xy12345.us-east-1`).
    pub(crate) fn parse(raw: &str) -> std::result::Result<Self, String> {
        let trimmed = raw.trim();
        let bare = trimmed.strip_prefix("https://").unwrap_or(trimmed);
        let lowered = bare.to_ascii_lowercase();
        let name = lowered
            .strip_suffix(SNOWFLAKE_HOST_SUFFIX)
            .unwrap_or(&lowered);
        let plain = |c: char| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.');
        if name.is_empty() || !name.chars().all(plain) || name.starts_with(['.', '-']) {
            return Err(format!(
                "`{}` is not a Snowflake account identifier — write `myorg-myaccount` or an \
                 account locator such as `xy12345.us-east-1`",
                raw.trim().escape_default()
            ));
        }
        Ok(Self {
            host: format!("{}{SNOWFLAKE_HOST_SUFFIX}", name.replace('_', "-")),
            jwt_name: jwt_account_name(name),
        })
    }

    pub(crate) fn endpoint(&self) -> String {
        format!("https://{}", self.host)
    }
}

/// The account part of the JWT claims: the identifier without its region, upper-cased.
fn jwt_account_name(name: &str) -> String {
    let cut = if name.contains(".global") {
        name.find('-')
    } else {
        name.find('.')
    };
    let account = match cut {
        Some(at) if at > 0 => &name[..at],
        _ => name,
    };
    account.to_ascii_uppercase()
}

#[cfg(test)]
mod tests {
    use super::Account;

    #[test]
    fn an_account_names_its_host_and_its_jwt_claims_the_way_snowflake_does() {
        for (raw, host, jwt) in [
            (
                "myorg-acct",
                "myorg-acct.snowflakecomputing.com",
                "MYORG-ACCT",
            ),
            (
                "MYORG-ACCT",
                "myorg-acct.snowflakecomputing.com",
                "MYORG-ACCT",
            ),
            (
                " myorg-acct ",
                "myorg-acct.snowflakecomputing.com",
                "MYORG-ACCT",
            ),
            (
                "https://myorg-acct.snowflakecomputing.com",
                "myorg-acct.snowflakecomputing.com",
                "MYORG-ACCT",
            ),
            (
                "xy12345.us-east-1",
                "xy12345.us-east-1.snowflakecomputing.com",
                "XY12345",
            ),
            (
                "xy12345.eu-central-1.aws",
                "xy12345.eu-central-1.aws.snowflakecomputing.com",
                "XY12345",
            ),
            (
                "my_org-acct",
                "my-org-acct.snowflakecomputing.com",
                "MY_ORG-ACCT",
            ),
            (
                "myorg-acct.global",
                "myorg-acct.global.snowflakecomputing.com",
                "MYORG",
            ),
        ] {
            let a = Account::parse(raw).unwrap_or_else(|e| panic!("{raw}: {e}"));
            assert_eq!((a.host.as_str(), a.jwt_name.as_str()), (host, jwt), "{raw}");
            assert_eq!(a.endpoint(), format!("https://{host}"), "{raw}");
        }
        for bad in [
            "",
            "  ",
            "a b",
            "acct/evil",
            ".x",
            "-x",
            "a@b",
            "acct\nx",
            "a:443",
        ] {
            let e = Account::parse(bad).unwrap_err();
            assert!(
                e.contains("not a Snowflake account identifier"),
                "{bad:?}: {e}"
            );
        }
    }
}
