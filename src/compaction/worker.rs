// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{snapshot_tracker::SnapshotTracker, stats::Stats, Keyspace};
use lsm_tree::AbstractTree;
use std::time::Instant;

/// Runs a single run of compaction.
pub fn run(
    keyspace: &Keyspace,
    snapshot_tracker: &SnapshotTracker,
    stats: &Stats,
) -> crate::Result<()> {
    use std::sync::atomic::Ordering::Relaxed;

    if keyspace.is_deleted.load(Relaxed) {
        return Ok(());
    }

    log::trace!(
        "Checking compaction strategy for keyspace {:?}",
        keyspace.name,
    );

    let strategy = keyspace.config.compaction_strategy.clone();

    stats.active_compaction_count.fetch_add(1, Relaxed);

    let start = Instant::now();

    if let Err(e) = keyspace
        .tree
        .compact(strategy.clone(), snapshot_tracker.get_seqno_safe_to_gc())
    {
        log::error!("Compaction failed: {e:?}");
        stats.active_compaction_count.fetch_sub(1, Relaxed);

        return Err(e.into());
    }

    // TODO: we need feedback from the compaction strategy...
    // TODO: if there is nothing more to do, we should clear the compaction_manager semaphore

    // NOTE: Throttle a bit to avoid a storm of compaction choice attempts
    // (in case of write throttling)
    std::thread::sleep(std::time::Duration::from_millis(1));

    #[expect(clippy::cast_possible_truncation)]
    stats
        .time_compacting
        .fetch_add(start.elapsed().as_micros() as u64, Relaxed);

    stats.active_compaction_count.fetch_sub(1, Relaxed);
    stats.compactions_completed.fetch_add(1, Relaxed);

    Ok(())
}
