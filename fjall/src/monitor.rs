use crate::{
    config::Config as KeyspaceConfig, flush::manager::FlushManager,
    journal::manager::JournalManager, Keyspace,
};
use std::sync::{atomic::AtomicU64, Arc, RwLock};

/// Monitors write buffer size & journal size
pub struct Monitor {
    pub(crate) keyspace_config: KeyspaceConfig,
    pub(crate) flush_manager: Arc<RwLock<FlushManager>>,
    pub(crate) journal_manager: Arc<RwLock<JournalManager>>,
    pub(crate) write_buffer_size: Arc<AtomicU64>,
}

impl Monitor {
    pub fn new(keyspace: &Keyspace) -> Self {
        Self {
            flush_manager: keyspace.flush_manager.clone(),
            journal_manager: keyspace.journal_manager.clone(),
            keyspace_config: keyspace.config.clone(),
            write_buffer_size: keyspace.approximate_write_buffer_size.clone(),
        }
    }

    pub fn run(&self) -> bool {
        let mut idle = true;

        let journal_manager = self.journal_manager.read().expect("lock is poisoned");
        let size = journal_manager.disk_space_used();

        if size as f64 > (self.keyspace_config.max_journaling_size_in_bytes as f64 * 0.5) {
            log::debug!(
                "monitor: try flushing affected partitions because journals have passed 50% of threshold"
            );
            idle = false;

            let partitions = journal_manager.get_partitions_to_flush_for_oldest_journal_eviction();
            drop(journal_manager);

            for partition in partitions {
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

        let write_buffer_size = self
            .write_buffer_size
            .load(std::sync::atomic::Ordering::Relaxed);

        if write_buffer_size as f64
            > (self.keyspace_config.max_write_buffer_size_in_bytes as f64 * 0.5)
        {
            log::trace!("monitor: flush inactive partition because write buffer has passed 50% of threshold");

            idle = false;

            // TODO: should flush biggest partition

            let least_recently_flush_partition = self
                .flush_manager
                .write()
                .expect("lock is poisoned")
                .get_least_recently_used_partition();

            if let Some(least_recently_flush_partition) = least_recently_flush_partition {
                if let Err(e) = least_recently_flush_partition.rotate_memtable() {
                    log::error!(
                        "monitor: memtable rotation failed for {:?}: {e:?}",
                        least_recently_flush_partition.name
                    );
                };
            };
        }

        idle
    }
}
