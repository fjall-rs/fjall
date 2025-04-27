// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::manager::CompactionManager;
use crate::{snapshot_tracker::SnapshotTracker, stats::Stats};
use lsm_tree::AbstractTree;
use std::time::Instant;

/// Runs a single run of compaction.
pub fn run(
    compaction_manager: &CompactionManager,
    snapshot_tracker: &SnapshotTracker,
    stats: &Stats,
) -> crate::Result<()> {
    use std::sync::atomic::Ordering::Relaxed;

    let Some(item) = compaction_manager.pop() else {
        return Ok(());
    };

    log::trace!(
        "compactor: calling compaction strategy for partition {:?}",
        item.0.name,
    );

    let strategy = item.config.compaction_strategy.clone();

    stats.active_compaction_count.fetch_add(1, Relaxed);

    let start = Instant::now();

    if let Err(e) = item
        .tree
        .compact(strategy.inner(), snapshot_tracker.get_seqno_safe_to_gc())
    {
        log::error!("Compaction failed: {e:?}");
        return Err(e.into());
    }

    #[allow(clippy::cast_possible_truncation)]
    stats
        .time_compacting
        .fetch_add(start.elapsed().as_micros() as u64, Relaxed);

    stats.active_compaction_count.fetch_sub(1, Relaxed);
    stats.compactions_completed.fetch_add(1, Relaxed);

    // TODO: loop if there's more work to do?

    Ok(())
}
