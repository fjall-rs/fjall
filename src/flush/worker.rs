// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{stats::Stats, supervisor::Supervisor, Keyspace};
use lsm_tree::AbstractTree;

/// Runs flush logic.
pub fn run(keyspace: &Keyspace, supervisor: &Supervisor, _stats: &Stats) -> crate::Result<()> {
    log::debug!("Flushing keyspace {:?}", keyspace.name());

    // TODO: take journal lock
    let _lock = supervisor
        .backpressure_lock
        .lock()
        .expect("lock should not be poisoned");

    let gc_watermark = supervisor.snapshot_tracker.get_seqno_safe_to_gc();

    // TODO: we probably don't need this anymore in lsm-tree
    // let flush_lock = keyspace.tree.get_flush_lock();

    let seqno = supervisor.seqno.next();

    let flushed_bytes = keyspace
        .version_set_provider
        .flush_active_memtable(&keyspace.tree, seqno, gc_watermark)
        .inspect_err(|e| {
            log::error!("Flush error: {e:?}");
        })?;

    log::debug!("Flush completed, freed {flushed_bytes} bytes from write buffer");

    Ok(())
}
