// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{snapshot_tracker::SnapshotTracker, stats::Stats, Keyspace};
use lsm_tree::{compaction::CompactionStrategy, AbstractTree};
use std::{sync::Arc, time::Instant};

/// Runs a compaction with the given strategy, and installs its result as a new version.
///
/// Can run next to other compactions, but not next to a major compaction, drop range or clear.
pub fn compact_with(
    keyspace: &Keyspace,
    strategy: Arc<dyn CompactionStrategy>,
) -> crate::Result<()> {
    let _compaction_lock = keyspace
        .compaction_lock
        .read()
        .map_err(|_| crate::Error::Poisoned)?;

    run_and_install(keyspace, strategy)
}

/// Like [`compact_with`], but no other compaction (or ingestion) runs at the same time,
/// e.g. for a major compaction.
pub fn compact_exclusive(
    keyspace: &Keyspace,
    strategy: Arc<dyn CompactionStrategy>,
) -> crate::Result<()> {
    let _compaction_lock = keyspace
        .compaction_lock
        .write()
        .map_err(|_| crate::Error::Poisoned)?;

    run_and_install(keyspace, strategy)
}

/// IMPORTANT: The caller must hold the keyspace's compaction lock until this returns,
/// so the compaction's input tables can't be picked by another compaction before
/// the result is installed
fn run_and_install(
    keyspace: &Keyspace,
    strategy: Arc<dyn CompactionStrategy>,
) -> crate::Result<()> {
    // NOTE: The deletion holds the compaction lock exclusively, so this can't race with it;
    // without the check, the compaction would write tables that nothing references
    if keyspace
        .is_deleted
        .load(std::sync::atomic::Ordering::Acquire)
    {
        return Err(crate::Error::KeyspaceDeleted);
    }

    let gc_watermark = keyspace.supervisor.snapshot_tracker.get_seqno_safe_to_gc();

    // NOTE: The merge is the expensive part, so it runs without holding the WAL or flush lock
    let result = keyspace.tree.run_compaction(strategy, gc_watermark)?;

    keyspace.install_version(|persister, seqno| {
        keyspace
            .version_set_provider
            .register_compaction(persister, result, seqno)
    })
}

/// Runs a single run of compaction.
pub fn run(
    keyspace: &Keyspace,
    _snapshot_tracker: &SnapshotTracker,
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

    log::debug!("Compacting keyspace {:?}", keyspace.name);

    let start = Instant::now();

    match compact_with(keyspace, strategy) {
        Ok(()) => {}
        Err(crate::Error::KeyspaceDeleted) => {
            // NOTE: The keyspace was deleted in the meantime, so there is nothing to compact
            log::debug!(
                "Skipping compaction of deleted keyspace {:?}",
                keyspace.name
            );
            stats.active_compaction_count.fetch_sub(1, Relaxed);
            return Ok(());
        }
        Err(e) => {
            log::error!("Compaction failed: {e:?}");
            stats.active_compaction_count.fetch_sub(1, Relaxed);
            return Err(e);
        }
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
