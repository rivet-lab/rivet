//! CONSEQUENCE: a preflight verdict (check, doctor, validate, init) is graded by the run it vouches for, never by its own words.
//! A green verdict must be followed by a delivering run (graded by the default oracle); a refusing one is paired with the run after it.

use super::remedy::{Said, ended, said};
use super::*;
use std::process::Output;

/// How the run after a refusing preflight must end.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Pairing {
    /// The run refuses with the preflight's own `[RIVET_*]` code and exit class.
    SameRefusal,
    /// The run refuses; the two name different codes by design.
    BothRefuse,
    /// A red verdict binds nothing: only a green one is held to its run.
    GreenOnly,
}

/// A preflight and the run after it, both finished and agreeing.
pub struct Consequence {
    pub preflight: Output,
    pub run: Output,
    /// Whether the preflight vouched for the run.
    pub green: bool,
}

/// Whether `out` vouches for a run: `all_ok` of a `doctor --json` report, else exit 0.
fn vouches(argv: &[&str], out: &Output) -> bool {
    if argv.first() != Some(&"doctor") || !argv.contains(&"--json") {
        return out.status.success();
    }
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap_or_else(|e| {
        panic!(
            "`rivet doctor --json` printed no JSON report ({e}):\n{}",
            String::from_utf8_lossy(&out.stdout)
        )
    });
    v["all_ok"]
        .as_bool()
        .unwrap_or_else(|| panic!("the doctor report has no `all_ok` bool: {v}"))
}

/// Why `run` does not follow from `pre`, else `Ok`.
pub(crate) fn agree(green: bool, pre: &Said, run: &Said, pairing: Pairing) -> Result<(), String> {
    let delivered = run.exit == Some(0);
    if green {
        return match delivered {
            true => Ok(()),
            false => Err(format!(
                "the preflight vouched for the run and the run refused: {}",
                ended(run)
            )),
        };
    }
    match pairing {
        Pairing::GreenOnly => Ok(()),
        Pairing::BothRefuse if delivered => Err(format!(
            "the preflight refused ({}) and the run delivered",
            ended(pre)
        )),
        Pairing::BothRefuse => Ok(()),
        Pairing::SameRefusal => {
            let Some(code) = pre.code.as_deref().filter(|_| pre.exit != Some(0)) else {
                return Err(format!(
                    "a paired refusal needs a coded preflight refusal, got {}",
                    ended(pre)
                ));
            };
            match run.code.as_deref() == Some(code) && run.exit == pre.exit {
                true => Ok(()),
                false => Err(format!(
                    "the preflight refused with {}, the run ended with {}",
                    ended(pre),
                    ended(run)
                )),
            }
        }
    }
}

/// Grade `pre` and `run` as one consequence; panics with both outputs when they disagree.
fn graded(argv: &[&str], pairing: Pairing, pre: Output, run: Output) -> Consequence {
    let green = vouches(argv, &pre);
    if let Err(why) = agree(green, &said(&pre), &said(&run), pairing) {
        panic!(
            "`rivet {}` and the run after it disagree: {why}\n--- preflight ---\n{}\n--- run ---\n{}",
            argv.join(" "),
            said(&pre).text,
            said(&run).text
        );
    }
    Consequence {
        preflight: pre,
        run,
        green,
    }
}

impl Rig {
    /// `rivet <preflight…>`, then the run it speaks for (`run_args` appended), held to `pairing`.
    pub fn run_after(
        &self,
        preflight: &[&str],
        run_args: &[&str],
        pairing: Pairing,
    ) -> Consequence {
        self.run_after_env(preflight, run_args, &[], pairing)
    }

    /// [`Rig::run_after`] with `envs` set on both legs.
    pub fn run_after_env(
        &self,
        preflight: &[&str],
        run_args: &[&str],
        envs: &[(&str, &str)],
        pairing: Pairing,
    ) -> Consequence {
        let pre = self.cli_env(preflight, envs);
        let run = self.run_args_env(run_args, envs);
        graded(preflight, pairing, pre, run)
    }
}

impl InitConfig {
    /// `rivet <preflight…>` over the generated config, then `rivet run <run_args…>` over it, held to `pairing`.
    pub fn run_after(
        &self,
        preflight: &[&str],
        run_args: &[&str],
        pairing: Pairing,
    ) -> Consequence {
        let pre = self.cli(preflight);
        let mut run_argv = vec!["run"];
        run_argv.extend_from_slice(run_args);
        let run = self.cli(&run_argv);
        graded(preflight, pairing, pre, run)
    }

    /// `check --export <export>`, then `run --export <export>` over the generated config, held to the same refusal.
    pub fn run_export_after_check(&self, export: &str) -> Consequence {
        self.run_after(
            &["check", "--export", export],
            &["--export", export],
            Pairing::SameRefusal,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn said_of(exit: i32, code: Option<&str>) -> Said {
        Said {
            exit: Some(exit),
            code: code.map(String::from),
            line: String::new(),
            text: String::new(),
        }
    }

    #[test]
    fn a_green_verdict_is_held_to_a_delivering_run() {
        let ok = said_of(0, None);
        let refused = said_of(2, Some("RIVET_X"));
        for p in [
            Pairing::SameRefusal,
            Pairing::BothRefuse,
            Pairing::GreenOnly,
        ] {
            assert!(agree(true, &ok, &ok, p).is_ok());
            assert!(agree(true, &ok, &refused, p).is_err());
        }
    }

    #[test]
    fn a_refusal_is_paired_by_its_pairing() {
        let pre = said_of(5, Some("RIVET_A"));
        let same = said_of(5, Some("RIVET_A"));
        let other_code = said_of(5, Some("RIVET_B"));
        let other_exit = said_of(2, Some("RIVET_A"));
        let uncoded = said_of(1, None);
        let ok = said_of(0, None);
        assert!(agree(false, &pre, &same, Pairing::SameRefusal).is_ok());
        assert!(agree(false, &pre, &other_code, Pairing::SameRefusal).is_err());
        assert!(agree(false, &pre, &other_exit, Pairing::SameRefusal).is_err());
        assert!(agree(false, &pre, &ok, Pairing::SameRefusal).is_err());
        assert!(agree(false, &uncoded, &uncoded, Pairing::SameRefusal).is_err());
        assert!(agree(false, &pre, &other_code, Pairing::BothRefuse).is_ok());
        assert!(agree(false, &pre, &uncoded, Pairing::BothRefuse).is_ok());
        assert!(agree(false, &pre, &ok, Pairing::BothRefuse).is_err());
        assert!(agree(false, &pre, &ok, Pairing::GreenOnly).is_ok());
    }

    #[test]
    fn a_doctor_report_vouches_by_all_ok_and_any_other_verb_by_exit() {
        let out = |code: i32, body: &str| Output {
            status: std::os::unix::process::ExitStatusExt::from_raw(code << 8),
            stdout: body.as_bytes().to_vec(),
            stderr: Vec::new(),
        };
        let doctor = ["doctor", "--json"];
        assert!(vouches(&doctor, &out(0, r#"{"all_ok": true}"#)));
        assert!(!vouches(&doctor, &out(0, r#"{"all_ok": false}"#)));
        assert!(vouches(&["check"], &out(0, "")));
        assert!(!vouches(&["check"], &out(2, "")));
    }

    #[test]
    #[should_panic(expected = "no `all_ok` bool")]
    fn a_doctor_report_without_all_ok_is_an_error_not_agreement() {
        let out = Output {
            status: std::os::unix::process::ExitStatusExt::from_raw(0),
            stdout: br#"{"ok": true}"#.to_vec(),
            stderr: Vec::new(),
        };
        vouches(&["doctor", "--json"], &out);
    }
}
