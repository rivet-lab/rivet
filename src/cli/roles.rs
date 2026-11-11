//! Command role registry: every clap leaf command has one role, and each role says which product invariants apply to it.

use super::args::{Cli, Commands, SchemaKind, StateAction};

/// What a leaf command is for; the role alone decides which invariants apply to it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    /// Passes or refuses a config before `pairs_with` runs it.
    PreRun { pairs_with: Leaf },
    /// Extracts and delivers the exports of a config.
    Run,
    /// Streams or loads data outside `run`.
    Delivery,
    /// Re-exports or fixes ranges of an existing delivery.
    Repair,
    /// Edits the state database.
    StateWrite,
    /// Reads state, journal, metrics or a delivery and prints a report.
    Report,
    /// Writes a config from a catalog read.
    Scaffold,
    /// Prints static text.
    Meta,
}

/// Which of the five product invariants (ADR-0040 D1) apply to a role.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Invariants {
    pub no_secret_in_output: bool,
    pub no_silent_null: bool,
    pub check_equals_run: bool,
    pub refusal_names_true_cause: bool,
    pub no_debris_after_stop: bool,
}

impl Role {
    /// The role's name as `rivet schema cli --json` prints it.
    pub const fn name(self) -> &'static str {
        match self {
            Role::PreRun { .. } => "pre_run",
            Role::Run => "run",
            Role::Delivery => "delivery",
            Role::Repair => "repair",
            Role::StateWrite => "state_write",
            Role::Report => "report",
            Role::Scaffold => "scaffold",
            Role::Meta => "meta",
        }
    }

    /// The invariants that apply to this role.
    pub const fn invariants(self) -> Invariants {
        let (secret, null, check, refusal, debris) = match self {
            Role::PreRun { .. } => (true, true, true, true, false),
            Role::Run => (true, true, true, true, true),
            Role::Delivery => (true, true, false, true, true),
            Role::Repair => (true, true, false, true, true),
            Role::StateWrite => (true, false, false, true, true),
            Role::Report => (true, false, false, true, false),
            Role::Scaffold => (true, false, false, true, true),
            Role::Meta => (false, false, false, false, false),
        };
        Invariants {
            no_secret_in_output: secret,
            no_silent_null: null,
            check_equals_run: check,
            refusal_names_true_cause: refusal,
            no_debris_after_stop: debris,
        }
    }

    /// The executor leaf a pre-run leaf is paired with.
    pub const fn pairs_with(self) -> Option<Leaf> {
        match self {
            Role::PreRun { pairs_with } => Some(pairs_with),
            Role::Run
            | Role::Delivery
            | Role::Repair
            | Role::StateWrite
            | Role::Report
            | Role::Scaffold
            | Role::Meta => None,
        }
    }
}

macro_rules! leaf_table {
    ($($variant:ident => $path:literal, $role:expr;)+) => {
        /// A leaf of the clap tree: a command with no subcommands of its own.
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        pub enum Leaf {
            $($variant),+
        }

        impl Leaf {
            /// Every leaf, in table order.
            pub const ALL: &'static [Leaf] = &[$(Leaf::$variant),+];

            /// The words after `rivet` that select this leaf.
            pub const fn path(self) -> &'static str {
                match self {
                    $(Leaf::$variant => $path),+
                }
            }

            /// The role of this leaf.
            pub const fn role(self) -> Role {
                match self {
                    $(Leaf::$variant => $role),+
                }
            }
        }
    };
}

leaf_table! {
    Run => "run", Role::Run;
    Check => "check", Role::PreRun { pairs_with: Leaf::Run };
    Doctor => "doctor", Role::PreRun { pairs_with: Leaf::Run };
    Cdc => "cdc", Role::Delivery;
    Load => "load", Role::Delivery;
    Compact => "compact", Role::Delivery;
    StateShow => "state show", Role::Report;
    StateReset => "state reset", Role::StateWrite;
    StateAccept => "state accept", Role::StateWrite;
    StateFiles => "state files", Role::Report;
    StateResetChunks => "state reset-chunks", Role::StateWrite;
    StateChunks => "state chunks", Role::Report;
    StateProgression => "state progression", Role::Report;
    StateRuns => "state runs", Role::Report;
    StateFinishRun => "state finish-run", Role::StateWrite;
    StateCopy => "state copy", Role::StateWrite;
    StateVacuum => "state vacuum", Role::StateWrite;
    StateLoads => "state loads", Role::Report;
    Completions => "completions", Role::Meta;
    Init => "init", Role::Scaffold;
    Plan => "plan", Role::PreRun { pairs_with: Leaf::Apply };
    Apply => "apply", Role::Run;
    Repair => "repair", Role::Repair;
    Validate => "validate", Role::Report;
    Reconcile => "reconcile", Role::Report;
    Metrics => "metrics", Role::Report;
    SchemaConfig => "schema config", Role::Meta;
    SchemaCli => "schema cli", Role::Meta;
    SchemaErrors => "schema errors", Role::Meta;
    Journal => "journal", Role::Report;
}

impl Commands {
    /// The leaf this parsed command selects; a new clap variant fails to compile here until it has a leaf.
    pub fn leaf(&self) -> Leaf {
        match self {
            Commands::Run(_) => Leaf::Run,
            Commands::Check { .. } => Leaf::Check,
            Commands::Doctor { .. } => Leaf::Doctor,
            Commands::Cdc { .. } => Leaf::Cdc,
            Commands::Load { .. } => Leaf::Load,
            Commands::Compact { .. } => Leaf::Compact,
            Commands::State { action } => match action {
                StateAction::Show { .. } => Leaf::StateShow,
                StateAction::Reset { .. } => Leaf::StateReset,
                StateAction::Accept { .. } => Leaf::StateAccept,
                StateAction::Files { .. } => Leaf::StateFiles,
                StateAction::ResetChunks { .. } => Leaf::StateResetChunks,
                StateAction::Chunks { .. } => Leaf::StateChunks,
                StateAction::Progression { .. } => Leaf::StateProgression,
                StateAction::Runs { .. } => Leaf::StateRuns,
                StateAction::FinishRun { .. } => Leaf::StateFinishRun,
                StateAction::Copy { .. } => Leaf::StateCopy,
                StateAction::Vacuum { .. } => Leaf::StateVacuum,
                StateAction::Loads { .. } => Leaf::StateLoads,
            },
            Commands::Completions { .. } => Leaf::Completions,
            Commands::Init { .. } => Leaf::Init,
            Commands::Plan(_) => Leaf::Plan,
            Commands::Apply { .. } => Leaf::Apply,
            Commands::Repair(_) => Leaf::Repair,
            Commands::Validate(_) => Leaf::Validate,
            Commands::Reconcile { .. } => Leaf::Reconcile,
            Commands::Metrics { .. } => Leaf::Metrics,
            Commands::Schema { what } => match what {
                SchemaKind::Config => Leaf::SchemaConfig,
                SchemaKind::Cli { .. } => Leaf::SchemaCli,
                SchemaKind::Errors => Leaf::SchemaErrors,
            },
            Commands::Journal { .. } => Leaf::Journal,
        }
    }
}

/// The registry as JSON: one object per leaf with its role, pair and applicable invariants.
pub fn registry_json() -> serde_json::Value {
    let leaves: Vec<serde_json::Value> = Leaf::ALL
        .iter()
        .map(|leaf| {
            let role = leaf.role();
            let inv = role.invariants();
            serde_json::json!({
                "path": leaf.path(),
                "role": role.name(),
                "pairs_with": role.pairs_with().map(Leaf::path),
                "invariants": {
                    "no_secret_in_output": inv.no_secret_in_output,
                    "no_silent_null": inv.no_silent_null,
                    "check_equals_run": inv.check_equals_run,
                    "refusal_names_true_cause": inv.refusal_names_true_cause,
                    "no_debris_after_stop": inv.no_debris_after_stop,
                },
            })
        })
        .collect();
    serde_json::json!({ "leaves": leaves })
}

/// The text `rivet schema cli` prints: the Markdown reference, or the role registry with `--json`.
pub fn schema_cli_text(json: bool) -> String {
    if json {
        let mut text = serde_json::to_string_pretty(&registry_json()).unwrap_or_default();
        text.push('\n');
        text
    } else {
        clap_markdown::help_markdown::<Cli>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::{CommandFactory, Parser};
    use std::collections::BTreeSet;

    fn clap_leaves(cmd: &clap::Command, prefix: &str, out: &mut Vec<(String, Vec<String>)>) {
        for sub in cmd.get_subcommands().filter(|s| s.get_name() != "help") {
            let path = if prefix.is_empty() {
                sub.get_name().to_string()
            } else {
                format!("{prefix} {}", sub.get_name())
            };
            if sub.has_subcommands() {
                clap_leaves(sub, &path, out);
            } else {
                out.push((path, minimal_argv(sub)));
            }
        }
    }

    fn dummy(arg: &clap::Arg) -> String {
        arg.get_possible_values()
            .first()
            .map_or_else(|| "x".to_string(), |v| v.get_name().to_string())
    }

    fn minimal_argv(sub: &clap::Command) -> Vec<String> {
        let mut argv = Vec::new();
        let mut grouped: BTreeSet<String> = BTreeSet::new();
        for group in sub.get_groups().filter(|g| g.is_required_set()) {
            if let Some(id) = group.get_args().next() {
                grouped.insert(id.to_string());
            }
        }
        for arg in sub.get_arguments() {
            let wanted = arg.is_required_set() || grouped.contains(arg.get_id().as_str());
            if !wanted || arg.get_id() == "help" || arg.get_id() == "version" {
                continue;
            }
            match arg.get_long() {
                Some(long) => {
                    argv.push(format!("--{long}"));
                    if arg.get_action().takes_values() {
                        argv.push(dummy(arg));
                    }
                }
                None => argv.push(dummy(arg)),
            }
        }
        argv
    }

    fn tree_leaves() -> Vec<(String, Vec<String>)> {
        let mut out = Vec::new();
        let mut root = Cli::command();
        root.build();
        clap_leaves(&root, "", &mut out);
        out
    }

    #[test]
    fn the_registry_holds_exactly_the_leaves_of_the_clap_tree() {
        let tree: BTreeSet<String> = tree_leaves().into_iter().map(|(p, _)| p).collect();
        let table: BTreeSet<String> = Leaf::ALL.iter().map(|l| l.path().to_string()).collect();
        assert_eq!(tree, table);
        assert_eq!(table.len(), Leaf::ALL.len(), "a path is listed twice");
    }

    #[test]
    fn every_parsed_leaf_maps_to_the_registry_entry_of_its_own_path() {
        for (path, args) in tree_leaves() {
            let mut argv = vec!["rivet".to_string()];
            argv.extend(path.split(' ').map(str::to_string));
            argv.extend(args);
            let cli = Cli::try_parse_from(&argv)
                .unwrap_or_else(|e| panic!("{argv:?} does not parse: {e}"));
            assert_eq!(cli.command.leaf().path(), path, "{argv:?}");
        }
    }

    #[test]
    fn a_pre_run_leaf_pairs_with_an_executor_leaf() {
        let mut pairs = Vec::new();
        for leaf in Leaf::ALL {
            if let Some(exec) = leaf.role().pairs_with() {
                assert_eq!(
                    exec.role(),
                    Role::Run,
                    "{} pairs with {}",
                    leaf.path(),
                    exec.path()
                );
                pairs.push((leaf.path(), exec.path()));
            }
        }
        assert_eq!(
            pairs,
            [("check", "run"), ("doctor", "run"), ("plan", "apply")]
        );
    }

    #[test]
    fn each_role_carries_its_invariants() {
        let rows = [
            (
                Role::PreRun {
                    pairs_with: Leaf::Run,
                },
                "pre_run",
                [true, true, true, true, false],
            ),
            (Role::Run, "run", [true, true, true, true, true]),
            (Role::Delivery, "delivery", [true, true, false, true, true]),
            (Role::Repair, "repair", [true, true, false, true, true]),
            (
                Role::StateWrite,
                "state_write",
                [true, false, false, true, true],
            ),
            (Role::Report, "report", [true, false, false, true, false]),
            (Role::Scaffold, "scaffold", [true, false, false, true, true]),
            (Role::Meta, "meta", [false, false, false, false, false]),
        ];
        for (role, name, want) in rows {
            let i = role.invariants();
            assert_eq!(role.name(), name);
            assert_eq!(
                [
                    i.no_secret_in_output,
                    i.no_silent_null,
                    i.check_equals_run,
                    i.refusal_names_true_cause,
                    i.no_debris_after_stop,
                ],
                want,
                "{name}"
            );
        }
    }

    #[test]
    fn the_json_lists_every_leaf_with_a_role_a_pair_and_five_invariants() {
        let json = registry_json();
        let leaves = json["leaves"].as_array().expect("leaves");
        assert_eq!(leaves.len(), Leaf::ALL.len());
        let check = leaves.iter().find(|l| l["path"] == "check").expect("check");
        assert_eq!(check["role"], "pre_run");
        assert_eq!(check["pairs_with"], "run");
        assert_eq!(check["invariants"]["check_equals_run"], true);
        assert_eq!(check["invariants"]["no_debris_after_stop"], false);
        let reset = leaves
            .iter()
            .find(|l| l["path"] == "state reset-chunks")
            .expect("state reset-chunks");
        assert_eq!(reset["role"], "state_write");
        assert!(reset["pairs_with"].is_null());
        assert_eq!(reset["invariants"].as_object().map(|o| o.len()), Some(5));
    }

    #[test]
    fn schema_cli_text_is_the_markdown_reference_unless_json_is_asked() {
        let md = schema_cli_text(false);
        assert_eq!(md, clap_markdown::help_markdown::<Cli>());
        let json: serde_json::Value = serde_json::from_str(&schema_cli_text(true)).expect("json");
        assert_eq!(json, registry_json());
        assert!(schema_cli_text(true).ends_with("}\n"));
    }
}
