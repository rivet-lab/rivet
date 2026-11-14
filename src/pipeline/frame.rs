//! The batch runner's opening frame — one seam instead of eight transcriptions.

use crate::destination::{self, Destination};
use crate::error::Result;
use crate::plan::ResolvedRunPlan;

/// The opened frame: the destination (already guarded) and the format's file extension.
pub(crate) struct RunnerFrame {
    pub dest: Box<dyn Destination>,
    pub ext: String,
}

impl RunnerFrame {
    /// Create the destination, refuse a cross-shape prefix (a batch manifest must never clobber a CDC export's audit trail.
    pub(crate) fn open(plan: &ResolvedRunPlan) -> Result<Self> {
        let frame = Self::open_unguarded(plan)?;
        crate::manifest::guard_manifest_mode(frame.dest.as_ref(), "batch")?;
        destination::log_capabilities(
            &plan.export_name,
            frame.dest.as_ref(),
            plan.destination.destination_type,
            plan.tuning.max_retries,
        );
        Ok(frame)
    }

    /// Refuse a prefix that holds another export shape's manifest, before any runner stores a plan, anchor or cursor.
    pub(crate) fn refuse_cross_shape_prefix(plan: &ResolvedRunPlan) -> Result<()> {
        let frame = Self::open_unguarded(plan)?;
        crate::manifest::guard_manifest_mode(frame.dest.as_ref(), "batch")
    }

    /// [`RunnerFrame::open`] for runners that share one destination across worker threads (parallel keyset / chunked / mongo).
    pub(crate) fn open_shared(
        plan: &ResolvedRunPlan,
    ) -> Result<(std::sync::Arc<Box<dyn Destination>>, String)> {
        let f = Self::open(plan)?;
        Ok((std::sync::Arc::new(f.dest), f.ext))
    }

    /// A destination + extension for a PER-ITERATION writer (each chunk / each worker), where the cross-shape guard has ALREADY fired at run start.
    pub(crate) fn open_unguarded(plan: &ResolvedRunPlan) -> Result<Self> {
        let dest = destination::create_destination(&plan.destination)?;
        let ext = crate::format::create_format(
            plan.format,
            plan.compression,
            plan.compression_level,
            None,
        )
        .file_extension()
        .to_string();
        Ok(Self { dest, ext })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::test_support::test_plan;

    fn local_plan(dir: &std::path::Path) -> ResolvedRunPlan {
        use crate::config::{DestinationConfig, DestinationType};
        ResolvedRunPlan {
            export_name: "frame_probe".into(),
            destination: DestinationConfig {
                destination_type: DestinationType::Local,
                path: Some(dir.to_string_lossy().into_owned()),
                ..Default::default()
            },
            ..test_plan()
        }
    }

    /// The invariant open_unguarded's safety rests on.
    #[test]
    fn unguarded_callers_also_guard_at_run_start() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/pipeline");
        let mut offenders = Vec::new();
        let mut stack = vec![root];
        while let Some(dir) = stack.pop() {
            for e in std::fs::read_dir(&dir).unwrap() {
                let path = e.unwrap().path();
                if path.is_dir() {
                    stack.push(path);
                    continue;
                }
                if path.extension().and_then(|x| x.to_str()) != Some("rs") {
                    continue;
                }
                let t = std::fs::read_to_string(&path).unwrap();
                if path.ends_with("frame.rs") {
                    continue;
                }
                if t.contains("open_unguarded(")
                    && !(t.contains("RunnerFrame::open(") || t.contains("open_shared("))
                {
                    offenders.push(path.display().to_string());
                }
            }
        }
        assert!(
            offenders.is_empty(),
            "open_unguarded without a run-start guarded open in the same module — \
             the per-iteration writer would have NO cross-shape guard:\n{}",
            offenders.join("\n")
        );
    }

    /// The frame's whole reason to exist: a destination obtained through it has ALREADY passed the cross-shape guard.
    #[test]
    fn open_refuses_a_cdc_prefix_before_any_part() {
        let dir = tempfile::tempdir().unwrap();
        let manifest = serde_json::json!({
            "manifest_version": 1,
            "export_name": "frame_probe",
            "mode": "cdc",
            "run_id": "r1",
            "status": "success",
            "parts": []
        });
        std::fs::write(
            dir.path().join("manifest.json"),
            serde_json::to_vec_pretty(&manifest).unwrap(),
        )
        .unwrap();

        let err = RunnerFrame::open(&local_plan(dir.path()))
            .err()
            .expect("a batch frame over a CDC prefix must refuse");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("cdc") || msg.contains("CDC"),
            "the refusal must name the cross-shape conflict: {msg}"
        );
    }

    /// The run-start guard refuses a CDC prefix and passes a clean or batch one.
    #[test]
    fn the_run_start_guard_refuses_a_cdc_prefix_and_passes_the_others() {
        let dir = tempfile::tempdir().unwrap();
        RunnerFrame::refuse_cross_shape_prefix(&local_plan(dir.path()))
            .expect("a clean prefix passes");
        for (mode, refused) in [("batch", false), ("cdc", true)] {
            let manifest = serde_json::json!({
                "manifest_version": 1, "export_name": "frame_probe", "mode": mode,
                "run_id": "r1", "status": "success", "parts": []
            });
            std::fs::write(
                dir.path().join("manifest.json"),
                serde_json::to_vec_pretty(&manifest).unwrap(),
            )
            .unwrap();
            let got = RunnerFrame::refuse_cross_shape_prefix(&local_plan(dir.path()));
            match (refused, got) {
                (false, Ok(())) => {}
                (true, Err(e)) => assert!(
                    format!("{e:#}").contains("already holds a 'cdc' manifest"),
                    "{e:#}"
                ),
                (_, other) => panic!("{mode}: refused={refused}, got {other:?}"),
            }
        }
    }

    /// …and a clean prefix opens, with the extension matching the format.
    #[test]
    fn open_yields_a_guarded_destination_and_the_formats_extension() {
        let dir = tempfile::tempdir().unwrap();
        let f = RunnerFrame::open(&local_plan(dir.path())).expect("clean prefix opens");
        assert_eq!(f.ext, "parquet");
        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), b"x").unwrap();
        f.dest
            .write(tmp.path(), "probe.bin")
            .expect("frame's destination writes");
        assert_eq!(std::fs::read(dir.path().join("probe.bin")).unwrap(), b"x");
    }
}
