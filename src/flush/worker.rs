// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    flush::Task, snapshot_tracker::SnapshotTracker, stats::Stats,
    write_buffer_manager::WriteBufferManager,
};
use lsm_tree::{AbstractTree, BlobFile, SeqNo, Table};

/// Flushes a single table.
fn run_flush_worker(
    task: &Task,
    eviction_threshold: SeqNo,
) -> crate::Result<Option<(Table, Option<BlobFile>)>> {
    // TODO: 3.0.0 opaque struct?
    Ok(task.keyspace.tree.flush_memtable(
        // IMPORTANT: Table has to get the task ID
        // otherwise table ID and memtable ID will not line up
        task.id,
        &task.sealed_memtable,
        eviction_threshold,
    )?)
}

/// Runs flush logic.
#[allow(clippy::too_many_lines)]
pub fn run(
    task: &Task,
    write_buffer_manager: &WriteBufferManager,
    snapshot_tracker: &SnapshotTracker,
    stats: &Stats,
) -> crate::Result<()> {
    let gc_watermark = snapshot_tracker.get_seqno_safe_to_gc();

    match run_flush_worker(task, gc_watermark) {
        Ok(Some((table, blob_file))) => {
            // TODO: API mismatch...
            let blob_files = blob_file.map(|x| vec![x]).unwrap_or_default();

            // IMPORTANT: Flushed tables need to be applied *atomically* into the tree
            // otherwise we could cover up an unwritten journal, which will result in data loss
            if let Err(e) = task
                .keyspace
                .tree
                .register_tables(&[table], Some(&blob_files), None)
            {
                log::error!("Failed to register tables: {e:?}");
                return Err(e.into());
            }

            write_buffer_manager.free(task.sealed_memtable.size());

            stats
                .flushes_completed
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

            task.keyspace
                .flushes_completed
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

            log::debug!("Flush completed");
        }
        Ok(None) => {
            log::trace!("Flush did not return a table");
        }
        Err(e) => {
            log::error!("Flush error: {e:?}");
            return Err(e);
        }
    }

    Ok(())
}
