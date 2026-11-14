//! **Layer: Execution** — parallel `_id`-range reader for MongoDB (OPT-4++).

use super::{RunSummary, commit};
use crate::config::IncrementalCursorMode;
use crate::error::Result;
use crate::plan::{IncrementalCursorPlan, KeysetPlan, ResolvedRunPlan};
use crate::source::mongo::MongoSource;
use crate::state::StateStore;

pub(crate) fn run_mongo_parallel(
    plan: &ResolvedRunPlan,
    summary: &mut RunSummary,
    state: &StateStore,
    kp: &KeysetPlan,
) -> Result<()> {
    let parallel = kp.parallel.max(1);
    let url = plan.source.resolve_url()?;
    let collection = crate::sql::strip_select_star_from(&plan.base_query).ok_or_else(|| {
        crate::error::coded(crate::error::codes::CONFIG_MODE_SETTING_INVALID, format!(
            "export '{}': parallel `_id`-range reads need a `table:` shortcut (a bare collection), \
             not a hand-written `query:`.",
            plan.export_name
        ))
    })?;

    let (ranges, pinned) = {
        let mut probe = MongoSource::connect(
            url.expose(),
            plan.source.tls.as_ref(),
            plan.source.mongo.as_ref(),
        )?;
        if let Some(incarnation) = plan.collection_incarnation.as_deref() {
            probe.pin_incarnation(collection, incarnation);
        }
        probe.confirm_collection(collection)?;
        (
            probe.sample_id_ranges(collection, parallel)?,
            probe.pinned(),
        )
    };
    log::info!(
        "export '{}': parallel `_id`-range read — {} range(s), page size {}",
        plan.export_name,
        ranges.len(),
        kp.chunk_size
    );

    let stamp = super::summary::run_scoped_stamp(&summary.run_id, &plan.export_name);

    let key_plan = IncrementalCursorPlan {
        primary_column: kp.key_column.clone(),
        fallback_column: None,
        mode: IncrementalCursorMode::SingleColumn,
        settle: None,
    };

    let (shared_dest, shared_ext) = super::frame::RunnerFrame::open_shared(plan)?;
    let fan = super::fan_in::FanIn::default();
    std::thread::scope(|s| {
        for (w, range) in ranges.iter().enumerate() {
            let (url, key_plan, stamp, ext, fan_r) = (&url, &key_plan, &stamp, &shared_ext, &fan);
            let pinned = pinned.clone();
            let (lo, hi) = (range.0.clone(), range.1.clone());
            let dest = std::sync::Arc::clone(&shared_dest);
            fan.spawn(s, format!("worker {w}"), move || {
                range_worker_pages(
                    url.expose(),
                    plan,
                    key_plan,
                    kp,
                    stamp,
                    w,
                    lo,
                    hi,
                    pinned,
                    dest,
                    ext,
                    fan_r,
                )
            });
        }
    });
    let drained = fan.finish(
        plan,
        summary,
        Some(state),
        None,
        |_, unit| match unit {
            commit::UnitId::Chunk(chunk_index) => commit::PartKind::Chunk { chunk_index },
            other => unreachable!("mongo parts are recorded under a chunk unit, not {other:?}"),
        },
        |errs| {
            crate::error::coded(
                crate::error::codes::RUN_UNITS_FAILED,
                format!(
                    "export '{}': parallel mongo failed on {} range(s): {}",
                    plan.export_name,
                    errs.len(),
                    errs.join("; ")
                ),
            )
        },
    );
    drained?;

    log::info!(
        "export '{}': parallel complete — {} range(s), {} rows",
        plan.export_name,
        ranges.len(),
        summary.total_rows
    );

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn range_worker_pages(
    url: &str,
    plan: &ResolvedRunPlan,
    key_plan: &IncrementalCursorPlan,
    kp: &KeysetPlan,
    stamp: &str,
    worker: usize,
    lo: mongodb::bson::Bson,
    hi: mongodb::bson::Bson,
    pinned: std::collections::HashMap<String, Vec<u8>>,
    dest: std::sync::Arc<Box<dyn crate::destination::Destination>>,
    ext: &str,
    fan: &super::fan_in::FanIn,
) -> Result<()> {
    let unit = commit::UnitId::Chunk(worker as i64);
    let mut src = MongoSource::connect(url, plan.source.tls.as_ref(), plan.source.mongo.as_ref())?
        .with_id_range(lo, hi)
        .with_pinned(pinned);

    let mut last: Option<String> = None;
    let mut page = 0usize;

    loop {
        if let Err(e) =
            crate::test_hook::maybe_error_at_index("mongo_parallel_worker", worker as i64)
        {
            return Err(crate::error::recoded(
                &e,
                crate::error::codes::CLI_TEST_FAULT_INJECTED,
                format!("range {worker}: {e}"),
            ));
        }
        let base = format!(
            "{}_{}_w{}_keyset{}.{}",
            plan.export_name, stamp, worker, page, ext
        );
        let (parts, p) = super::keyset::read_keyset_page(
            &mut src,
            plan,
            key_plan,
            kp.chunk_size,
            last.as_deref(),
            &**dest,
            &base,
        );
        for part in parts {
            fan.part(unit, part);
        }
        let Some(p) = p? else {
            break;
        };
        fan.observe(p.observed);
        fan.contribute(unit, p.checksums);
        let next = super::keyset::next_seek(
            last.as_deref(),
            p.rows,
            kp.chunk_size,
            p.next_cursor,
            &kp.key_column,
            page,
            &format!("export '{}': parallel worker {worker}", plan.export_name),
        )?;
        page += 1;
        match next {
            Some(v) => last = Some(v),
            None => break,
        }
    }
    Ok(())
}
