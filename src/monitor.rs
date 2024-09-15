// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    config::Config as KeyspaceConfig,
    flush::manager::{FlushManager, Task as FlushTask},
    journal::{manager::JournalManager, Journal},
    keyspace::Partitions,
    write_buffer_manager::WriteBufferManager,
    Keyspace,
};
use lsm_tree::AbstractTree;
use std::sync::{Arc, RwLock};
use std_semaphore::Semaphore;

/// Monitors write buffer size & journal size
pub struct Monitor {
    pub(crate) flush_manager: Arc<RwLock<FlushManager>>,
    pub(crate) keyspace_config: KeyspaceConfig,
    pub(crate) journal_manager: Arc<RwLock<JournalManager>>,
    pub(crate) write_buffer_manager: WriteBufferManager,
    pub(crate) partitions: Arc<RwLock<Partitions>>,
    pub(crate) journal: Arc<Journal>,
    pub(crate) flush_semaphore: Arc<Semaphore>,
}

impl Drop for Monitor {
    fn drop(&mut self) {
        log::trace!("Dropping monitor");

        #[cfg(feature = "__internal_whitebox")]
        crate::drop::decrement_drop_counter();
    }
}

impl Monitor {
    pub fn new(keyspace: &Keyspace) -> Self {
        #[cfg(feature = "__internal_whitebox")]
        crate::drop::increment_drop_counter();

        Self {
            flush_manager: keyspace.flush_manager.clone(),
            journal_manager: keyspace.journal_manager.clone(),
            keyspace_config: keyspace.config.clone(),
            write_buffer_manager: keyspace.write_buffer_manager.clone(),
            partitions: keyspace.partitions.clone(),
            journal: keyspace.journal.clone(),
            flush_semaphore: keyspace.flush_semaphore.clone(),
        }
    }

    fn try_reduce_journal_size(&self) {
        log::debug!(
            "monitor: try flushing affected partitions because journals have passed 50% of threshold"
        );

        let mut journal_writer = self.journal.get_writer();
        let mut journal_manager = self.journal_manager.write().expect("lock is poisoned");

        let seqno_map = journal_manager.rotate_partitions_to_flush_for_oldest_journal_eviction();

        if seqno_map.is_empty() {
            self.flush_semaphore.release();

            if let Err(e) = journal_manager.maintenance() {
                log::error!("journal GC failed: {e:?}");
            };
        } else {
            log::debug!(
                "monitor: need to flush {} partitions to evict oldest journal",
                seqno_map.len()
            );

            let partitions_names_with_queued_tasks = self
                .flush_manager
                .read()
                .expect("lock is poisoned")
                .get_partitions_with_tasks();

            let actual_seqno_map = seqno_map
                .iter()
                .map(|(_, seqno, _, _)| seqno)
                .cloned()
                .collect::<Vec<_>>();

            #[allow(clippy::collapsible_if)]
            if actual_seqno_map
                .iter()
                .any(|x| !partitions_names_with_queued_tasks.contains(&x.partition.name))
            {
                if journal_manager
                    .rotate_journal(&mut journal_writer, actual_seqno_map)
                    .is_ok()
                {
                    let mut flush_manager = self.flush_manager.write().expect("lock is poisoned");

                    for (partition, _, yanked_id, yanked_memtable) in seqno_map {
                        flush_manager.enqueue_task(
                            partition.name.clone(),
                            FlushTask {
                                id: yanked_id,
                                partition,
                                sealed_memtable: yanked_memtable,
                            },
                        );
                    }

                    self.flush_semaphore.release();

                    drop(flush_manager);
                    drop(journal_manager);
                    drop(journal_writer);
                }
            } else {
                self.flush_semaphore.release();

                if let Err(e) = journal_manager.maintenance() {
                    log::error!("journal GC failed: {e:?}");
                }
            }
        }
    }

    fn try_reduce_write_buffer_size(&self) {
        log::trace!(
            "monitor: flush inactive partition because write buffer has passed 50% of threshold"
        );

        let mut partitions = self
            .partitions
            .read()
            .expect("lock is poisoned")
            .values()
            .cloned()
            .collect::<Vec<_>>();

        partitions.sort_by(|a, b| {
            b.tree
                .active_memtable_size()
                .cmp(&a.tree.active_memtable_size())
        });

        let partitions_names_with_queued_tasks = self
            .flush_manager
            .read()
            .expect("lock is poisoned")
            .get_partitions_with_tasks();

        let partitions = partitions
            .into_iter()
            .filter(|x| !partitions_names_with_queued_tasks.contains(&x.name));

        for partition in partitions {
            log::debug!("monitor: WB rotating {:?}", partition.name);

            match partition.rotate_memtable() {
                Ok(rotated) => {
                    if rotated {
                        break;
                    }
                }
                Err(e) => {
                    log::error!(
                        "monitor: memtable rotation failed for {:?}: {e:?}",
                        partition.name
                    );
                }
            };
        }
    }

    pub fn run(&self) -> bool {
        let mut idle = true;

        let jm_size = self
            .journal_manager
            .read()
            .expect("lock is poisoned")
            .disk_space_used();

        let max_journal_size = self.keyspace_config.max_journaling_size_in_bytes;

        if jm_size as f64 > (max_journal_size as f64 * 0.5) {
            self.try_reduce_journal_size();
            idle = false;
        }

        let write_buffer_size = self.write_buffer_manager.get();

        let queued_size = self
            .flush_manager
            .read()
            .expect("lock is poisoned")
            .queued_size();

        // TODO: This should never ever overflow
        // TODO: because that is definitely a logic error
        // TODO: need to make sure it's impossible this can happen
        #[cfg(debug_assertions)]
        {
            // NOTE: Cannot use panic because we are in a thread that should not
            // crash
            if queued_size > write_buffer_size {
                log::error!(
                    "Queued size should not be able to be greater than entire write buffer size"
                );
                return idle;
            }
        }

        // NOTE: We cannot flush more stuff if the journal is already too large
        if jm_size < max_journal_size {
            let max_write_buffer_size = self.keyspace_config.max_write_buffer_size_in_bytes;

            // NOTE: Take the queued size of unflushed memtables into account
            // so the system isn't performing a flush storm once the threshold is reached
            //
            // Also, As a fail safe, use saturating_sub so it doesn't overflow
            let buffer_size_without_queued_size = write_buffer_size.saturating_sub(queued_size);

            if buffer_size_without_queued_size as f64 > (max_write_buffer_size as f64 * 0.5) {
                self.try_reduce_write_buffer_size();
                idle = false;
            }
        } else {
            log::debug!("cannot rotate memtable to free write buffer - journal too large");
        }

        idle
    }
}
