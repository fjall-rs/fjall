// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    flush::Task, snapshot_tracker::SnapshotTracker, stats::Stats,
    write_buffer_manager::WriteBufferManager,
};
use lsm_tree::AbstractTree;

/// Runs flush logic.
#[allow(clippy::too_many_lines)]
pub fn run(
    task: &Task,
    write_buffer_manager: &WriteBufferManager,
    snapshot_tracker: &SnapshotTracker,
    stats: &Stats,
) -> crate::Result<()> {
    let gc_watermark = snapshot_tracker.get_seqno_safe_to_gc();

    let flush_lock = task.keyspace.tree.get_flush_lock();

    match task
        .keyspace
        .tree
        .flush(&flush_lock, gc_watermark)
        .inspect_err(|e| {
            log::error!("Flush error: {e:?}");
        })? {
        Some(result) => {
            // TODO: 3.0.0 lsm-tree needs to give us number of bytes
            todo!();

            write_buffer_manager.free(0);

            stats
                .flushes_completed
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

            task.keyspace
                .flushes_completed
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

            log::debug!("Flush completed");
        }
        None => {
            log::trace!("Flush did not return a table");
        }
    }

    Ok(())
}
