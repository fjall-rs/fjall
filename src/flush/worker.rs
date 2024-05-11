use super::manager::{FlushManager, Task};
use crate::{
    batch::PartitionKey, compaction::manager::CompactionManager, file::SEGMENTS_FOLDER,
    journal::manager::JournalManager, write_buffer_manager::WriteBufferManager, PartitionHandle,
};
use lsm_tree::Segment;
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

/// Flushes a single segment.
fn run_flush_worker(task: &Arc<Task>) -> crate::Result<Arc<Segment>> {
    use lsm_tree::flush::Options;

    let segment = lsm_tree::flush::flush_to_segment(Options {
        tree_id: task.partition.tree.id,

        // IMPORTANT: Segment has to get the task ID
        // otherwise segment ID and memtable ID will not line up
        segment_id: task.id,

        memtable: task.sealed_memtable.clone(),
        folder: task.partition.tree.path.join(SEGMENTS_FOLDER),
        block_size: task.partition.tree.config.block_size,
        block_cache: task.partition.tree.block_cache.clone(),
        descriptor_table: task.partition.tree.descriptor_table.clone(),
    })?;

    Ok(Arc::new(segment))
}

struct MultiFlushResultItem {
    partition: PartitionHandle,
    created_segments: Vec<Arc<Segment>>,

    /// Size sum of sealed memtables that have been flushed
    size: u64,
}

type MultiFlushResults = Vec<crate::Result<MultiFlushResultItem>>;

/// Distributes tasks of multiple partitions over multiple worker threads.
///
/// Each thread is responsible for the tasks of one partition.
fn run_multi_flush(partitioned_tasks: &HashMap<PartitionKey, Vec<Arc<Task>>>) -> MultiFlushResults {
    log::debug!(
        "flush worker: spawning {} worker threads",
        partitioned_tasks.len()
    );

    // NOTE: Don't trust clippy
    #[allow(clippy::needless_collect)]
    let threads = partitioned_tasks
        .iter()
        .map(|(partition_name, tasks)| {
            let partition_name = partition_name.clone();
            let tasks = tasks.clone();

            std::thread::spawn(move || {
                log::trace!(
                    "flush thread: flushing {} memtables for partition {partition_name:?}",
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
                    .map(|task| std::thread::spawn(move || run_flush_worker(&task)))
                    .collect::<Vec<_>>();

                let created_segments = flush_workers
                    .into_iter()
                    .map(|t| t.join().expect("should join"))
                    .collect::<crate::Result<Vec<_>>>()?;

                Ok(MultiFlushResultItem {
                    partition,
                    created_segments,
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
    parallelism: usize,
) {
    log::debug!("flush worker: write locking flush manager");
    let mut fm = flush_manager.write().expect("lock is poisoned");
    let partitioned_tasks = fm.collect_tasks(parallelism);
    drop(fm);

    let task_count = partitioned_tasks.iter().map(|x| x.1.len()).sum::<usize>();

    if task_count == 0 {
        log::debug!("flush worker: No tasks collected");
        return;
    }

    for result in run_multi_flush(&partitioned_tasks) {
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
                } else {
                    log::debug!("flush worker: write locking flush manager to submit results");
                    let mut flush_manager = flush_manager.write().expect("lock is poisoned");
                    flush_manager.dequeue_tasks(partition.name.clone(), created_segments.len());

                    write_buffer_manager.free(memtables_size);
                    compaction_manager.notify(partition);
                }
            }
            Err(e) => {
                log::error!("Flush error: {e:?}");
            }
        }
    }

    log::debug!("flush worker: write locking journal manager to maybe do maintenance");
    if let Err(e) = journal_manager
        .write()
        .expect("lock is poisoned")
        .maintenance()
    {
        log::error!("journal GC failed: {e:?}");
    };

    log::debug!("flush worker: fully done");
}
