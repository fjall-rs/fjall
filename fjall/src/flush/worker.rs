use crate::{file::SEGMENTS_FOLDER, Keyspace};
use std::sync::Arc;

/// Runs flush worker.
///
/// Only spawn one of these, it will internally spawn worker threads as needed.
pub fn run(keyspace: &Keyspace) {
    loop {
        keyspace.flush_semaphore.acquire();

        let mut flush_manager = keyspace.flush_manager.write().expect("lock is poisoned");
        let partitioned_tasks =
            flush_manager.collect_tasks(1 /* TODO: parallelism, CPU cores probably */);
        drop(flush_manager);

        log::trace!("Spawning {} worker threads", partitioned_tasks.len());

        let threads = partitioned_tasks
            .iter()
            .map(|(partition_name, tasks)| {
                let partition_name = partition_name.clone();
                let tasks = tasks.clone();

                std::thread::spawn(move || {
                    log::trace!(
                        "Flush thread, Flushing {} memtables for partition {partition_name:?}",
                        tasks.len()
                    );

                    let partition = tasks
                        .first()
                        .expect("should always have at least one task")
                        .partition
                        .clone();

                    // NOTE: Don't trust clippy
                    #[allow(clippy::needless_collect)]
                    let flush_workers = tasks
                        .into_iter()
                        .map(|task| {
                            std::thread::spawn(move || {
                                use lsm_tree::flush::Options;

                                let segment = lsm_tree::flush_to_segment(Options {
                                    memtable: task.sealed_memtable.clone(),
                                    segment_id: task.id.clone(),
                                    folder: task
                                        .partition
                                        .tree
                                        .config
                                        .path
                                        .join(SEGMENTS_FOLDER)
                                        .join(&*task.id),
                                    block_size: task.partition.tree.config.block_size,
                                    block_cache: task.partition.tree.config.block_cache.clone(),
                                })?;
                                let segment = Arc::new(segment);

                                Ok::<_, crate::Error>(segment)
                            })
                        })
                        .collect::<Vec<_>>();

                    let results = flush_workers
                        .into_iter()
                        .map(|t| t.join().expect("should join"))
                        .collect::<crate::Result<Vec<_>>>()?;

                    Ok::<_, crate::Error>((partition, results))
                })
            })
            .collect::<Vec<_>>();

        let results = threads
            .into_iter()
            .map(|t| t.join().expect("should join"))
            .collect::<Vec<_>>();

        log::trace!("All flush worker threads are done");
        let mut flush_manager = keyspace.flush_manager.write().expect("lock is poisoned");

        // TODO: handle flush fail
        for result in results {
            match result {
                Ok((partition, segments)) => {
                    // IMPORTANT: Flushed segments need to be applied *atomically* into the tree
                    // otherwise we could cover up an unwritten journal, which will result in data loss

                    match partition.tree.register_segments(&segments) {
                        Ok(()) => {
                            for segment in segments {
                                flush_manager.dequeue_task(partition.name.clone());
                                partition.tree.free_sealed_memtable(&segment.metadata.id);
                            }

                            keyspace.compaction_manager.notify(partition);
                        }
                        Err(e) => {
                            log::error!("Failed to register segments: {e:?}");
                        }
                    }
                }
                Err(e) => {
                    log::error!("Flush error: {e:?}");
                }
            }
        }

        drop(flush_manager);

        if let Err(e) = keyspace
            .journal_manager
            .write()
            .expect("lock is poisoned")
            .maintenance()
        {
            log::error!("journal GC failed: {e:?}");
        };

        // TODO: check for deleted partitions
    }
}
