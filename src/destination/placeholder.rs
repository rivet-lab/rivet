//! **Layer: Coordinator helper** (destination prefix templating)

use chrono::{NaiveDate, Utc};

use crate::config::DestinationConfig;

/// Substitution context shared by every command that resolves destination prefixes.
#[derive(Debug, Clone)]
pub struct PlaceholderContext {
    /// Date used for `{date}` substitution; `None` leaves the literal `{date}` in place for the apply that resolves it.
    pub date: Option<NaiveDate>,
    /// Export name used for `{export}` and `{table}`.
    pub export_name: String,
    /// Optional run identifier used for `{run_id}`.
    pub run_id: Option<String>,
}

impl PlaceholderContext {
    /// Context anchored at today's UTC date.
    pub fn for_today(export_name: impl Into<String>) -> Self {
        Self {
            date: Some(Utc::now().date_naive()),
            export_name: export_name.into(),
            run_id: None,
        }
    }

    /// Context that resolves the names and keeps `{date}` as written.
    pub fn keeping_date(export_name: impl Into<String>) -> Self {
        Self {
            date: seeded!(
                "sealed-plan-freezes-the-date",
                Some(Utc::now().date_naive()),
                None
            ),
            export_name: export_name.into(),
            run_id: None,
        }
    }

    /// Context anchored at an explicit date.
    pub fn for_date(date: NaiveDate, export_name: impl Into<String>) -> Self {
        Self {
            date: Some(date),
            export_name: export_name.into(),
            run_id: None,
        }
    }

    /// Attach a run identifier for `{run_id}` substitution.
    pub fn with_run_id(mut self, run_id: impl Into<String>) -> Self {
        self.run_id = Some(run_id.into());
        self
    }
}

/// Apply placeholder substitution to a single string.
pub fn apply(s: &str, ctx: &PlaceholderContext) -> String {
    let mut out = s
        .replace("{export}", &ctx.export_name)
        .replace("{table}", &ctx.export_name);
    if let Some(date) = ctx.date {
        out = out.replace("{date}", &date.format("%Y-%m-%d").to_string());
    }
    if let Some(rid) = &ctx.run_id {
        out = out.replace("{run_id}", rid);
    }
    out
}

/// The text of `template` around the placeholders [`apply`] substitutes, in order; `None` when it holds none.
pub fn literal_parts(template: &str) -> Option<Vec<String>> {
    const CUT: &str = "\u{0}";
    let cut = ["{date}", "{export}", "{table}", "{run_id}"]
        .iter()
        .fold(template.to_string(), |t, token| t.replace(token, CUT));
    cut.contains(CUT)
        .then(|| cut.split(CUT).map(str::to_string).collect())
}

/// Substitute **only** the `{partition}` token in `destination.path` / `destination.prefix` with a Hive-style `col=value` segment.
pub fn expand_destination_partition(
    mut dest: DestinationConfig,
    segment: &str,
) -> DestinationConfig {
    dest.path = dest.path.map(|s| s.replace("{partition}", segment));
    dest.prefix = dest.prefix.map(|s| s.replace("{partition}", segment));
    dest
}

/// Expand placeholders in `destination.path` and `destination.prefix`.
pub fn expand_destination(
    mut dest: DestinationConfig,
    ctx: &PlaceholderContext,
) -> DestinationConfig {
    dest.path = dest.path.map(|s| apply(&s, ctx));
    dest.prefix = dest.prefix.map(|s| apply(&s, ctx));
    dest
}

/// `dest` resolved as of `ctx`, with the destination as written when the two differ (the progress identity reads it, so a new day's prefix is not a moved destination).
pub fn resolve_destination(
    dest: DestinationConfig,
    ctx: &PlaceholderContext,
) -> (DestinationConfig, Option<String>) {
    let written = dest.uri();
    let resolved = expand_destination(dest, ctx);
    let written = Some(written).filter(|w| *w != resolved.uri());
    (resolved, written)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::DestinationType;
    use chrono::NaiveDate;

    fn ctx_on(date_str: &str, export: &str) -> PlaceholderContext {
        PlaceholderContext::for_date(
            NaiveDate::parse_from_str(date_str, "%Y-%m-%d").unwrap(),
            export,
        )
    }

    #[test]
    fn literal_parts_are_the_text_around_each_substituted_placeholder() {
        let own = |p: &[&str]| Some(p.iter().map(|s| s.to_string()).collect::<Vec<_>>());
        assert_eq!(
            literal_parts("runs/{date}/{export}/x/{table}{run_id}"),
            own(&["runs/", "/", "/x/", "", ""])
        );
        assert_eq!(literal_parts("{date}"), own(&["", ""]));
        assert_eq!(literal_parts("runs/{partition}/{unknown}/"), None);
        assert_eq!(literal_parts("runs/plain/"), None);
    }

    #[test]
    fn apply_substitutes_date_export_table() {
        let ctx = ctx_on("2026-05-21", "orders");
        assert_eq!(
            apply("runs/{date}/{export}/{table}", &ctx),
            "runs/2026-05-21/orders/orders",
        );
    }

    #[test]
    fn apply_leaves_unknown_tokens_verbatim() {
        let ctx = ctx_on("2026-05-21", "orders");
        assert_eq!(
            apply("runs/{date}/{unknown}/", &ctx),
            "runs/2026-05-21/{unknown}/",
        );
    }

    #[test]
    fn apply_run_id_only_when_set() {
        let ctx = ctx_on("2026-05-21", "orders");
        assert_eq!(apply("runs/{run_id}/", &ctx), "runs/{run_id}/");

        let ctx = ctx.with_run_id("r-abc123");
        assert_eq!(apply("runs/{run_id}/", &ctx), "runs/r-abc123/");
    }

    #[test]
    fn apply_is_idempotent_on_already_expanded_string() {
        let ctx = ctx_on("2026-05-21", "orders");
        let once = apply("runs/{date}/{export}/", &ctx);
        let twice = apply(&once, &ctx);
        assert_eq!(once, twice);
        assert_eq!(once, "runs/2026-05-21/orders/");
    }

    #[test]
    fn expand_destination_rewrites_path_and_prefix() {
        let dest = DestinationConfig {
            destination_type: DestinationType::S3,
            prefix: Some("exports/{date}/{export}/".into()),
            path: Some("/scratch/{table}/{date}".into()),
            ..Default::default()
        };
        let ctx = ctx_on("2026-05-21", "orders");
        let expanded = expand_destination(dest, &ctx);
        assert_eq!(
            expanded.prefix.as_deref(),
            Some("exports/2026-05-21/orders/")
        );
        assert_eq!(expanded.path.as_deref(), Some("/scratch/orders/2026-05-21"));
    }

    #[test]
    fn expand_destination_no_placeholders_unchanged() {
        let dest = DestinationConfig {
            destination_type: DestinationType::Local,
            path: Some("./out".into()),
            ..Default::default()
        };
        let ctx = ctx_on("2026-05-21", "orders");
        let expanded = expand_destination(dest, &ctx);
        assert_eq!(expanded.path.as_deref(), Some("./out"));
        assert!(expanded.prefix.is_none());
    }

    #[test]
    fn keeping_date_resolves_the_names_and_leaves_date_for_the_apply() {
        let ctx = PlaceholderContext::keeping_date("orders");
        assert_eq!(
            apply("runs/{date}/{export}/{table}", &ctx),
            "runs/{date}/orders/orders"
        );
        let later = apply(
            &apply("runs/{date}/{export}/", &ctx),
            &ctx_on("2026-05-22", "orders"),
        );
        assert_eq!(later, "runs/2026-05-22/orders/");
    }

    #[test]
    fn resolve_destination_keeps_the_destination_as_written_only_when_it_differs() {
        let dest = DestinationConfig {
            destination_type: DestinationType::S3,
            bucket: Some("b".into()),
            prefix: Some("runs/{date}/{export}/".into()),
            ..Default::default()
        };
        let (kept, written) =
            resolve_destination(dest.clone(), &PlaceholderContext::keeping_date("orders"));
        assert_eq!(kept.prefix.as_deref(), Some("runs/{date}/orders/"));
        assert_eq!(written.as_deref(), Some("s3://b/runs/{date}/{export}/"));
        let (frozen, written) = resolve_destination(dest, &ctx_on("2026-05-21", "orders"));
        assert_eq!(frozen.prefix.as_deref(), Some("runs/2026-05-21/orders/"));
        assert_eq!(written.as_deref(), Some("s3://b/runs/{date}/{export}/"));
        let plain = DestinationConfig {
            destination_type: DestinationType::Local,
            path: Some("/out/2026-05-21".into()),
            ..Default::default()
        };
        let (same, written) = resolve_destination(plain.clone(), &ctx_on("2026-05-22", "orders"));
        assert_eq!(same.path, plain.path);
        assert_eq!(written, None);
    }

    #[test]
    fn expand_destination_with_run_id() {
        let dest = DestinationConfig {
            destination_type: DestinationType::S3,
            prefix: Some("runs/{date}/{run_id}/{export}/".into()),
            ..Default::default()
        };
        let ctx = ctx_on("2026-05-21", "orders").with_run_id("r-abc123");
        let expanded = expand_destination(dest, &ctx);
        assert_eq!(
            expanded.prefix.as_deref(),
            Some("runs/2026-05-21/r-abc123/orders/"),
        );
    }
}
