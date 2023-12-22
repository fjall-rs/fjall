use crate::{file::SEGMENTS_FOLDER, Keyspace};
use std::sync::Arc;
use std_semaphore::Semaphore;

pub fn start(keyspace: Keyspace, flush_semaphore: Arc<Semaphore>) {
    std::thread::spawn(move || loop {
        flush_semaphore.acquire();

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

                    let partition = tasks[0].partition.clone();

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

        for thread in threads {
            // TODO: handle flush fail
            // TODO: need to partially send results to flush manager (per partition)
            let (partition, results) = thread
                .join()
                .expect("should join")
                .expect("flush failed :(");

            // TODO: handle this
            partition
                .tree
                .register_segments(&results)
                .expect("should not fail");
        }

        log::trace!("All worker threads are done");

        let mut flush_manager = keyspace.flush_manager.write().expect("lock is poisoned");

        for (partition_name, tasks) in partitioned_tasks {
            for task in tasks {
                flush_manager.dequeue_task(partition_name.clone());
                task.partition.tree.free_sealed_memtable(&task.id);
            }
        }

        if let Err(e) = keyspace
            .journal_manager
            .write()
            .expect("lock is poisoned")
            .maintenance()
        {
            log::error!("journal GC failed: {e:?}");
        };
    });
}
