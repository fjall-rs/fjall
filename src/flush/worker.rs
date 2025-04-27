// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::manager::{FlushManager, Task};
use crate::{
    batch::PartitionKey, compaction::manager::CompactionManager, journal::manager::JournalManager,
    snapshot_tracker::SnapshotTracker, stats::Stats, write_buffer_manager::WriteBufferManager,
    HashMap, PartitionHandle,
};
use lsm_tree::{AbstractTree, Segment, SeqNo};
use std::sync::{Arc, RwLock};

/// Flushes a single segment.
fn run_flush_worker(task: &Arc<Task>, eviction_threshold: SeqNo) -> crate::Result<Option<Segment>> {
    #[rustfmt::skip]
    let segment = task.partition.tree.flush_memtable(
        // IMPORTANT: Segment has to get the task ID
        // otherwise segment ID and memtable ID will not line up
        task.id,
        &task.sealed_memtable,
        eviction_threshold,
    );

    // TODO: test this after a failed flush
    if segment.is_err() {
        // IMPORTANT: Need to decrement pending segments counter
        if let crate::AnyTree::Blob(tree) = &task.partition.tree {
            tree.pending_segments
                .fetch_sub(1, std::sync::atomic::Ordering::Release);
        }
    }

    Ok(segment?)
}

struct MultiFlushResultItem {
    partition: PartitionHandle,
    created_segments: Vec<Segment>,

    /// Size sum of sealed memtables that have been flushed
    size: u64,
}

type MultiFlushResults = Vec<crate::Result<MultiFlushResultItem>>;

/// Distributes tasks of multiple partitions over multiple worker threads.
///
/// Each thread is responsible for the tasks of one partition.
fn run_multi_flush(
    partitioned_tasks: &HashMap<PartitionKey, Vec<Arc<Task>>>,
    eviction_threshold: SeqNo,
) -> MultiFlushResults {
    log::debug!("spawning {} worker threads", partitioned_tasks.len());

    // NOTE: Don't trust clippy
    #[allow(clippy::needless_collect)]
    let threads = partitioned_tasks
        .iter()
        .map(|(partition_name, tasks)| {
            let partition_name = partition_name.clone();
            let tasks = tasks.clone();

            std::thread::spawn(move || {
                log::trace!(
                    "flushing {} memtables for partition {partition_name:?}",
                    tasks.len()
                );

                let partition = tasks
                    .first()
                    .expect("should always have at least one task")
                    .partition
                    .clone();

                let memtables_size: u64 = tasks
                    .iter()
                    .map(|t| u64::from(t.sealed_memtable.size()))
                    .sum();

                // NOTE: Don't trust clippy
                #[allow(clippy::needless_collect)]
                let flush_workers = tasks
                    .into_iter()
                    .map(|task| {
                        std::thread::spawn(move || run_flush_worker(&task, eviction_threshold))
                    })
                    .collect::<Vec<_>>();

                let created_segments = flush_workers
                    .into_iter()
                    .map(|t| t.join().expect("should join"))
                    .collect::<crate::Result<Vec<_>>>()?;

                Ok(MultiFlushResultItem {
                    partition,
                    created_segments: created_segments.into_iter().flatten().collect(),
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

    for result in run_multi_flush(&partitioned_tasks, snapshot_tracker.get_seqno_safe_to_gc()) {
        match result {
            Ok(MultiFlushResultItem {
                partition,
                created_segments,
                size: memtables_size,
            }) => {
                // IMPORTANT: Flushed segments need to be applied *atomically* into the tree
                // otherwise we could cover up an unwritten journal, which will result in data loss
                if let Err(e) = partition.tree.register_segments(&created_segments) {
                    log::error!("Failed to register segments: {e:?}");
                    return Err(e.into());
                }

                log::debug!("write locking flush manager to submit results");
                let mut flush_manager = flush_manager.write().expect("lock is poisoned");

                log::debug!(
                    "Dequeuing flush tasks: {} => {}",
                    partition.name,
                    created_segments.len(),
                );
                flush_manager.dequeue_tasks(partition.name.clone(), created_segments.len());

                write_buffer_manager.free(memtables_size);

                for _ in 0..parallelism {
                    compaction_manager.notify(partition.clone());
                }

                stats
                    .flushes_completed
                    .fetch_add(created_segments.len(), std::sync::atomic::Ordering::Relaxed);

                partition
                    .flushes_completed
                    .fetch_add(created_segments.len(), std::sync::atomic::Ordering::Relaxed);
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
