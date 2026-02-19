// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    flush::Task, snapshot_tracker::SnapshotTracker, stats::Stats,
    write_buffer_manager::WriteBufferManager,
};
use lsm_tree::AbstractTree;

/// Runs flush logic.
pub fn run(
    task: &Task,
    write_buffer_manager: &WriteBufferManager,
    snapshot_tracker: &SnapshotTracker,
    _stats: &Stats,
) -> crate::Result<()> {
    log::debug!("Flushing keyspace {:?}", task.keyspace.name);

    let gc_watermark = snapshot_tracker.get_seqno_safe_to_gc();

    let flush_lock = task.keyspace.tree.get_flush_lock();

    match task
        .keyspace
        .tree
        .flush(&flush_lock, gc_watermark)
        .inspect_err(|e| {
            log::error!("Flush error: {e:?}");
        })? {
        Some(flushed_bytes) => {
            write_buffer_manager.free(flushed_bytes);

            log::debug!("Flush completed");
        }
        None => {
            log::trace!("Flush did not return a table");
        }
    }

    Ok(())
}
