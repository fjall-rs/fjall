// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{db::Keyspaces, supervisor::Supervisor, Config};
use lsm_tree::AbstractTree;
use std::{
    sync::{Arc, RwLock},
    time::Duration,
};

pub fn handle_journal(
    supervisor: &Supervisor,
    keyspaces: &Arc<RwLock<Keyspaces>>,
    db_config: &Config,
    is_poisoned: &std::sync::atomic::AtomicBool,
) {
    let start = std::time::Instant::now();

    // TODO: maybe if 50% are reached we can look at some memtables that are close to being full

    if {
        supervisor
            .journal_manager
            .read()
            .expect("lock is poisoned")
            .disk_space_used()
    } > (db_config.max_journaling_size_in_bytes / 4 * 3)
    {
        let _lock = supervisor
            .backpressure_lock
            .lock()
            .expect("lock is poisoned");

        let stragglers = supervisor
            .journal_manager
            .read()
            .expect("lock is poisoned")
            .get_keyspaces_to_flush_for_oldest_journal_eviction();

        for keyspace in stragglers {
            log::info!("Rotating {:?} to try to reduce journal size", keyspace.name);
            keyspace.request_rotation();
        }
    }

    while {
        supervisor
            .journal_manager
            .read()
            .expect("lock is poisoned")
            .disk_space_used()
    } > db_config.max_journaling_size_in_bytes
    {
        std::thread::sleep(std::time::Duration::from_millis(10));

        if start.elapsed() > std::time::Duration::from_secs(5) {
            log::info!("Halting writes for 5+ secs now because journal is still too large");

            // TODO: this may not scale well for many keyspaces
            {
                let _lock = supervisor
                    .backpressure_lock
                    .lock()
                    .expect("lock is poisoned");

                let stragglers = supervisor
                    .journal_manager
                    .read()
                    .expect("lock is poisoned")
                    .get_keyspaces_to_flush_for_oldest_journal_eviction();

                for keyspace in stragglers {
                    log::info!("Rotating {:?} to try to reduce journal size", keyspace.name);
                    keyspace.request_rotation();
                }

                /* let keyspaces = keyspaces.read().expect("lock is poisoned");

                let mut keyspaces_with_seqno = keyspaces
                    .values()
                    .filter(|x| x.tree.active_memtable().size() > 0)
                    .map(|x| (x.clone(), x.tree.get_highest_persisted_seqno()))
                    .collect::<Vec<_>>();

                drop(keyspaces);

                keyspaces_with_seqno.sort_by(|a, b| a.1.cmp(&b.1));

                if let Some(lowest) = keyspaces_with_seqno.first() {
                    log::debug!("Rotating {:?} to try to reduce journal size", lowest.0.name);
                    lowest.0.request_rotation();
                } */
            }

            if start.elapsed() > std::time::Duration::from_secs(10) {
                supervisor.snapshot_tracker.pullup();
                supervisor.snapshot_tracker.gc();
                break;
            }

            if start.elapsed() > std::time::Duration::from_secs(30) {
                log::debug!("Giving up after {:?}", start.elapsed());
                break;
            }

            supervisor
                .journal_manager
                .write()
                .expect("lock is poisoned")
                .maintenance();

            std::thread::sleep(Duration::from_millis(490));
        }

        if is_poisoned.load(std::sync::atomic::Ordering::Relaxed) {
            log::error!("DB was poisoned while being write halted");
            return;
        }
    }
}

pub fn handle_write_buffer(
    supervisor: &Supervisor,
    keyspaces: &Arc<RwLock<Keyspaces>>,
    db_config: &Config,
    is_poisoned: &std::sync::atomic::AtomicBool,
) {
    let start = std::time::Instant::now();

    loop {
        let wb_size = supervisor.write_buffer_size.get();

        // TODO: maybe if 50% are reached we can look at some memtables that are close to being full

        if wb_size > (db_config.max_write_buffer_size_in_bytes / 4 * 3) {
            let _lock = supervisor
                .backpressure_lock
                .lock()
                .expect("lock is poisoned");

            let keyspaces = keyspaces.read().expect("lock is poisoned");

            let mut keyspaces_with_sizes: Vec<(_, u64)> = keyspaces
                .values()
                .filter(|x| x.tree.active_memtable().size() > 0)
                .map(|x| (x.clone(), x.tree.active_memtable().size()))
                .collect::<Vec<_>>();

            drop(keyspaces);

            keyspaces_with_sizes.sort_by(|a, b| a.1.cmp(&b.1));

            let mut queued_bytes = 0;

            for (keyspace, bytes) in keyspaces_with_sizes.iter().rev() {
                log::debug!(
                    "Rotating {:?} to try to reduce database write buffer size",
                    keyspace.name,
                );
                keyspace.request_rotation();

                queued_bytes += bytes;
            }

            break;
        }

        if wb_size <= db_config.max_write_buffer_size_in_bytes {
            break;
        }

        let overshoot = wb_size - db_config.max_write_buffer_size_in_bytes;

        std::thread::sleep(std::time::Duration::from_millis(10));

        if start.elapsed() > std::time::Duration::from_secs(3) {
            log::info!(
                "Halting writes for {:?} now because database write buffer is still too large",
                start.elapsed(),
            );
            log::debug!("Write buffer overshoot is {overshoot}B");

            // TODO: we should somehow register that we have already queued flushes so we don't overqueue
            {
                let _lock = supervisor
                    .backpressure_lock
                    .lock()
                    .expect("lock is poisoned");

                let keyspaces = keyspaces.read().expect("lock is poisoned");

                let mut keyspaces_with_sizes: Vec<(_, u64)> = keyspaces
                    .values()
                    .filter(|x| x.tree.active_memtable().size() > 0)
                    .map(|x| (x.clone(), x.tree.active_memtable().size()))
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
                    keyspace.request_rotation();

                    queued_bytes += bytes;
                }
            }

            if start.elapsed() > std::time::Duration::from_secs(10) {
                supervisor.snapshot_tracker.pullup();
                supervisor.snapshot_tracker.gc();
                break;
            }

            if start.elapsed() > std::time::Duration::from_secs(30) {
                log::debug!("Giving up after {:?}", start.elapsed());
                break;
            }

            std::thread::sleep(Duration::from_millis(490));
        }

        if is_poisoned.load(std::sync::atomic::Ordering::Relaxed) {
            log::error!("DB was poisoned while being write halted");
            return;
        }
    }
}
