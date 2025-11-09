// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::manager::{FlushManager, Task};
use crate::{
    compaction::manager::CompactionManager, journal::manager::JournalManager,
    keyspace::InternalKeyspaceId, snapshot_tracker::SnapshotTracker, stats::Stats,
    write_buffer_manager::WriteBufferManager, HashMap, Keyspace,
};
use lsm_tree::{AbstractTree, BlobFile, SeqNo, Table};
use std::sync::{Arc, RwLock};

/// Flushes a single table.
fn run_flush_worker(
    task: &Arc<Task>,
    eviction_threshold: SeqNo,
) -> crate::Result<Option<(Table, Option<BlobFile>)>> {
    Ok(task.keyspace.tree.flush_memtable(
        // IMPORTANT: Table has to get the task ID
        // otherwise table ID and memtable ID will not line up
        task.id,
        &task.sealed_memtable,
        eviction_threshold,
    )?) // TODO: 3.0.0 opaque struct
}

struct MultiFlushResultItem {
    keyspace: Keyspace,
    created_tables: Vec<(Table, Option<BlobFile>)>,

    /// Size sum of sealed memtables that have been flushed
    size: u64,
}

type MultiFlushResults = Vec<crate::Result<MultiFlushResultItem>>;

/// Distributes tasks of multiple keyspaces over multiple worker threads.
///
/// Each thread is responsible for the tasks of one keyspace.
fn run_multi_flush(
    partitioned_tasks: &HashMap<InternalKeyspaceId, Vec<Arc<Task>>>,
    eviction_threshold: SeqNo,
) -> MultiFlushResults {
    log::debug!("spawning {} worker threads", partitioned_tasks.len());

    // NOTE: Don't trust clippy
    #[allow(clippy::needless_collect)]
    let threads = partitioned_tasks
        .iter()
        .map(|(keyspace_id, tasks)| {
            let keyspace_id = *keyspace_id;
            let tasks = tasks.clone();

            std::thread::spawn(move || {
                log::trace!(
                    "flushing {} memtables for keyspace {keyspace_id}",
                    tasks.len()
                );

                let keyspace = tasks
                    .first()
                    .expect("should always have at least one task")
                    .keyspace
                    .clone();

                let memtables_size: u64 = tasks.iter().map(|t| t.sealed_memtable.size()).sum();

                // NOTE: Don't trust clippy
                #[allow(clippy::needless_collect)]
                let flush_workers = tasks
                    .into_iter()
                    .map(|task| {
                        std::thread::spawn(move || run_flush_worker(&task, eviction_threshold))
                    })
                    .collect::<Vec<_>>();

                let created_tables = flush_workers
                    .into_iter()
                    .map(|t| t.join().expect("should join"))
                    .collect::<crate::Result<Vec<_>>>()?;

                Ok(MultiFlushResultItem {
                    keyspace,
                    created_tables: created_tables.into_iter().flatten().collect(),
                    size: memtables_size,
                })
            })
        })
        .collect::<Vec<_>>();

    threads
        .into_iter()
        .map(|t| t.join().expect("should join"))
        .collect::<Vec<_>>()
}

/// Runs flush logic.
#[allow(clippy::too_many_lines)]
pub fn run(
    flush_manager: &Arc<RwLock<FlushManager>>,
    journal_manager: &Arc<RwLock<JournalManager>>,
    compaction_manager: &CompactionManager,
    write_buffer_manager: &WriteBufferManager,
    snapshot_tracker: &SnapshotTracker,
    parallelism: usize,
    stats: &Stats,
) -> crate::Result<()> {
    log::debug!("write locking flush manager");
    let mut fm = flush_manager.write().expect("lock is poisoned");
    let partitioned_tasks = fm.collect_tasks(parallelism);
    drop(fm);

    let task_count = partitioned_tasks.iter().map(|x| x.1.len()).sum::<usize>();

    if task_count == 0 {
        log::debug!("No tasks collected");
        return Ok(());
    }

    let gc_watermark = snapshot_tracker.get_seqno_safe_to_gc();

    for result in run_multi_flush(&partitioned_tasks, gc_watermark) {
        match result {
            Ok(MultiFlushResultItem {
                keyspace,
                created_tables,
                size: memtables_size,
            }) => {
                // TODO: 3.0.0 this should all be handled in lsm-tree
                // TODO: by making the result of flushes an opaque struct
                // TODO: and allowing to merge multiple flush results
                //
                let (created_tables, blob_files) = created_tables.into_iter().rev().fold(
                    (vec![], vec![]),
                    |(mut tables, mut blob_files), (sst, bf)| {
                        tables.push(sst);
                        blob_files.extend(bf);
                        (tables, blob_files)
                    },
                );

                // IMPORTANT: Flushed tables need to be applied *atomically* into the tree
                // otherwise we could cover up an unwritten journal, which will result in data loss
                if let Err(e) =
                    keyspace
                        .tree
                        .register_tables(&created_tables, Some(&blob_files), None)
                {
                    log::error!("Failed to register tables: {e:?}");
                    return Err(e.into());
                }

                log::debug!("write locking flush manager to submit results");
                let mut flush_manager = flush_manager.write().expect("lock is poisoned");

                log::debug!(
                    "Dequeuing flush tasks: {} => {}",
                    keyspace.name,
                    created_tables.len(),
                );
                flush_manager.dequeue_tasks(keyspace.id, created_tables.len());

                write_buffer_manager.free(memtables_size);

                for _ in 0..parallelism {
                    compaction_manager.notify(keyspace.clone());
                }

                stats
                    .flushes_completed
                    .fetch_add(created_tables.len(), std::sync::atomic::Ordering::Relaxed);

                keyspace
                    .flushes_completed
                    .fetch_add(created_tables.len(), std::sync::atomic::Ordering::Relaxed);
            }
            Err(e) => {
                log::error!("Flush error: {e:?}");
                return Err(e);
            }
        }
    }

    log::debug!("write locking journal manager to maybe do maintenance");
    if let Err(e) = journal_manager
        .write()
        .expect("lock is poisoned")
        .maintenance()
    {
        log::error!("journal GC failed: {e:?}");
        return Err(e);
    }

    log::debug!("fully done");

    Ok(())
}
