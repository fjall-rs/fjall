use crate::{flush::Task as FlushTask, worker_pool::WorkerMessage, Keyspace};
use lsm_tree::AbstractTree;
use std::{sync::Arc, time::Duration};

pub fn handle_journal(keyspace: &Keyspace) {
    let start = std::time::Instant::now();

    while {
        keyspace
            .supervisor
            .journal_manager
            .read()
            .expect("lock is poisoned")
            .disk_space_used()
    } > keyspace.db_config.max_journaling_size_in_bytes
    {
        std::thread::sleep(std::time::Duration::from_millis(10));

        if start.elapsed() > std::time::Duration::from_secs(5) {
            log::debug!("Halting writes for 5+ secs now because journal is still too large");

            // TODO: this may not scale well for many keyspaces
            {
                let _lock = keyspace
                    .supervisor
                    .backpressure_lock
                    .lock()
                    .expect("lock is poisoned");

                let keyspaces = keyspace.keyspaces.read().expect("lock is poisoned");

                let mut keyspaces_with_seqno = keyspaces
                    .values()
                    .filter(|x| x.tree.active_memtable_size() > 0)
                    .map(|x| (x.clone(), x.tree.get_highest_persisted_seqno()))
                    .collect::<Vec<_>>();

                drop(keyspaces);

                keyspaces_with_seqno.sort_by(|a, b| a.1.cmp(&b.1));

                if let Some(lowest) = keyspaces_with_seqno.first() {
                    log::debug!("Rotating {:?} to try to reduce journal size", lowest.0.name,);

                    match lowest.0.rotate_memtable() {
                        Ok(_) => {
                            keyspace
                                .supervisor
                                .flush_manager
                                .enqueue(Arc::new(FlushTask {
                                    keyspace: lowest.0.clone(),
                                }));
                            keyspace.worker_messager.try_send(WorkerMessage::Flush).ok();
                        }
                        Err(e) => {
                            log::warn!("Rotating keyspace {:?} failed: {e:?}", lowest.0.name,);
                        }
                    }
                }
            }

            if start.elapsed() > std::time::Duration::from_secs(10) {
                keyspace.supervisor.snapshot_tracker.pullup();
                keyspace.supervisor.snapshot_tracker.gc();
                break;
            }

            if start.elapsed() > std::time::Duration::from_secs(30) {
                log::debug!("Giving up after {:?}", start.elapsed());
                break;
            }

            std::thread::sleep(Duration::from_millis(490));
        }

        if keyspace
            .is_poisoned
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            log::error!("DB was poisoned while being write halted");
            return;
        }
    }
}

pub fn handle_write_buffer(keyspace: &Keyspace) {
    let start = std::time::Instant::now();

    loop {
        let wb_size = keyspace.supervisor.write_buffer_size.get();

        if wb_size <= keyspace.db_config.max_write_buffer_size_in_bytes {
            break;
        }

        let overshoot = wb_size - keyspace.db_config.max_write_buffer_size_in_bytes;

        std::thread::sleep(std::time::Duration::from_millis(10));

        if start.elapsed() > std::time::Duration::from_secs(3) {
            log::debug!(
                "Halting writes for {:?} now because database write buffer is still too large",
                start.elapsed()
            );
            log::debug!("Write buffer overshoot is {overshoot}B");

            // TODO: we should somehow register that we have already queued flushes so we don't overqueue
            {
                let _lock = keyspace
                    .supervisor
                    .backpressure_lock
                    .lock()
                    .expect("lock is poisoned");

                let keyspaces = keyspace.keyspaces.read().expect("lock is poisoned");

                let mut keyspaces_with_sizes: Vec<(_, u64)> = keyspaces
                    .values()
                    .filter(|x| x.tree.active_memtable_size() > 0)
                    .map(|x| (x.clone(), x.tree.active_memtable_size()))
                    .collect::<Vec<_>>();

                drop(keyspaces);

                keyspaces_with_sizes.sort_by(|a, b| a.1.cmp(&b.1));

                let mut queued_bytes = 0;

                for (keyspace, bytes) in keyspaces_with_sizes.iter().rev() {
                    if queued_bytes >= overshoot {
                        break;
                    }

                    log::debug!(
                        "Rotating {:?} to try to reduce database write buffer size",
                        keyspace.name,
                    );

                    match keyspace.rotate_memtable() {
                        Ok(_) => {
                            keyspace
                                .supervisor
                                .flush_manager
                                .enqueue(Arc::new(FlushTask {
                                    keyspace: keyspace.clone(),
                                }));
                            keyspace.worker_messager.try_send(WorkerMessage::Flush).ok();
                        }
                        Err(e) => {
                            log::warn!("Rotating keyspace {:?} failed: {e:?}", keyspace.name,);
                        }
                    }

                    queued_bytes += bytes;
                }
            }

            if start.elapsed() > std::time::Duration::from_secs(10) {
                keyspace.supervisor.snapshot_tracker.pullup();
                keyspace.supervisor.snapshot_tracker.gc();
                break;
            }

            if start.elapsed() > std::time::Duration::from_secs(30) {
                log::debug!("Giving up after {:?}", start.elapsed());
                break;
            }

            std::thread::sleep(Duration::from_millis(490));
        }

        if keyspace
            .is_poisoned
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            log::error!("DB was poisoned while being write halted");
            return;
        }
    }
}
