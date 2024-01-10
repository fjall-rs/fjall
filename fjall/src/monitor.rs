use crate::{
    config::Config as KeyspaceConfig, flush::manager::FlushManager,
    journal::manager::JournalManager, keyspace::Partitions, Keyspace,
};
use std::sync::{atomic::AtomicU64, Arc, RwLock};

/// Monitors write buffer size & journal size
pub struct Monitor {
    pub(crate) keyspace_config: KeyspaceConfig,
    pub(crate) journal_manager: Arc<RwLock<JournalManager>>,
    pub(crate) write_buffer_size: Arc<AtomicU64>,
    pub(crate) partitions: Arc<RwLock<Partitions>>,
}

impl Monitor {
    pub fn new(keyspace: &Keyspace) -> Self {
        Self {
            journal_manager: keyspace.journal_manager.clone(),
            keyspace_config: keyspace.config.clone(),
            write_buffer_size: keyspace.approximate_write_buffer_size.clone(),
            partitions: keyspace.partitions.clone(),
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

            for partition in partitions {
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

        idle
    }
}
