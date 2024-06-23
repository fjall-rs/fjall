use crate::{
    config::Config as KeyspaceConfig, flush::manager::FlushManager,
    journal::manager::JournalManager, keyspace::Partitions,
    write_buffer_manager::WriteBufferManager, Keyspace,
};
use std::sync::{Arc, RwLock};

/// Monitors write buffer size & journal size
pub struct Monitor {
    pub(crate) flush_manager: Arc<RwLock<FlushManager>>,
    pub(crate) keyspace_config: KeyspaceConfig,
    pub(crate) journal_manager: Arc<RwLock<JournalManager>>,
    pub(crate) write_buffer_manager: WriteBufferManager,
    pub(crate) partitions: Arc<RwLock<Partitions>>,
}

impl Drop for Monitor {
    fn drop(&mut self) {
        log::trace!("Dropping monitor");
    }
}

impl Monitor {
    pub fn new(keyspace: &Keyspace) -> Self {
        Self {
            flush_manager: keyspace.flush_manager.clone(),
            journal_manager: keyspace.journal_manager.clone(),
            keyspace_config: keyspace.config.clone(),
            write_buffer_manager: keyspace.write_buffer_manager.clone(),
            partitions: keyspace.partitions.clone(),
        }
    }

    pub fn run(&self) -> bool {
        let mut idle = true;

        let journal_manager = self.journal_manager.read().expect("lock is poisoned");
        let size = journal_manager.disk_space_used();

        if size as f64 > (self.keyspace_config.max_journaling_size_in_bytes as f64 * 0.5) {
            idle = false;

            log::debug!(
                "monitor: try flushing affected partitions because journals have passed 50% of threshold"
            );

            let partitions = journal_manager.get_partitions_to_flush_for_oldest_journal_eviction();
            drop(journal_manager);

            // NOTE: Don't try to flush partitions that are already enqueued in the flush manager
            // to prevent a flush storm once the threshold is reached
            let partition_names_with_queued_tasks = self
                .flush_manager
                .read()
                .expect("lock is poisoned")
                .get_partitions_with_tasks();

            let partitions = partitions
                .into_iter()
                .filter(|x| !partition_names_with_queued_tasks.contains(&x.name));

            for partition in partitions {
                log::debug!("monitor: JM rotating {:?}", partition.name);

                if let Err(e) = partition.rotate_memtable() {
                    log::error!(
                        "monitor: memtable rotation failed for {:?}: {e:?}",
                        partition.name
                    );
                };
            }
        } else {
            drop(journal_manager);
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

        // NOTE: Take the queued size of unflushed memtables into account
        // so the system isn't performing a flush storm once the threshold is reached
        //
        // Also, As a fail safe, use saturating_sub so it doesn't overflow
        let buffer_size_without_queued_size = write_buffer_size.saturating_sub(queued_size);

        if buffer_size_without_queued_size as f64
            > (self.keyspace_config.max_write_buffer_size_in_bytes as f64 * 0.5)
        {
            log::trace!("monitor: flush inactive partition because write buffer has passed 50% of threshold");

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

            idle = false;
        }

        idle
    }
}
