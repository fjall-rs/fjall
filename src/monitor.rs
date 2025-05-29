// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    background_worker::Activity, config::Config as KeyspaceConfig, flush::manager::FlushManager,
    journal::manager::JournalManager, keyspace::Partitions, snapshot_tracker::SnapshotTracker,
    write_buffer_manager::WriteBufferManager, Keyspace,
};
use lsm_tree::{AbstractTree, SequenceNumberCounter};
use std::{
    sync::{atomic::AtomicBool, Arc, RwLock},
    time::Duration,
};

// TODO: remove in favor of just checking in the write path
// TODO: and sending signals to background workers

/// Monitors write buffer size & journal size
pub struct Monitor {
    pub(crate) flush_manager: Arc<RwLock<FlushManager>>,
    pub(crate) keyspace_config: KeyspaceConfig,
    pub(crate) journal_manager: Arc<RwLock<JournalManager>>,
    pub(crate) write_buffer_manager: WriteBufferManager,
    pub(crate) partitions: Arc<RwLock<Partitions>>,
    pub(crate) seqno: SequenceNumberCounter,
    pub(crate) snapshot_tracker: SnapshotTracker,
    pub(crate) keyspace_poison: Arc<AtomicBool>,
}

impl Activity for Monitor {
    fn name(&self) -> &'static str {
        "monitor"
    }

    fn run(&mut self) -> crate::Result<()> {
        std::thread::sleep(Duration::from_millis(250));

        // TODO: don't do this too often
        let current_seqno = self.seqno.get();
        let gc_seqno_watermark = self.snapshot_tracker.get_seqno_safe_to_gc();

        // NOTE: If the difference between watermark is too large, and
        // we never opened a snapshot, we need to pull the watermark up
        //
        // https://github.com/fjall-rs/fjall/discussions/85
        if (current_seqno - gc_seqno_watermark) > 100 && self.snapshot_tracker.data.is_empty() {
            *self
                .snapshot_tracker
                .lowest_freed_instant
                .write()
                .expect("lock is poisoned") = current_seqno.saturating_sub(100);
        }

        let jm_size = self
            .journal_manager
            .read()
            .expect("lock is poisoned")
            .disk_space_used();

        let max_journal_size = self.keyspace_config.max_journaling_size_in_bytes;

        if jm_size as f64 > (max_journal_size as f64 * 0.5) {
            self.try_reduce_journal_size();
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
                return Ok(());
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
            }
        } else {
            log::debug!("cannot rotate memtable to free write buffer - journal too large");
        }

        Ok(())
    }
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
            seqno: keyspace.seqno.clone(),
            snapshot_tracker: keyspace.snapshot_tracker.clone(),
            keyspace_poison: keyspace.is_poisoned.clone(),
        }
    }

    fn try_reduce_journal_size(&self) {
        log::debug!(
            "monitor: try flushing affected partitions because journals have passed 50% of threshold"
        );

        let partitions = self.partitions.read().expect("lock is poisoned");

        // TODO: this may not scale well for many partitions
        let lowest_persisted_partition = partitions
            .values()
            .filter(|x| x.tree.active_memtable_size() > 0)
            .min_by(|a, b| {
                a.tree
                    .get_highest_persisted_seqno()
                    .cmp(&b.tree.get_highest_persisted_seqno())
            })
            .cloned();

        drop(partitions);

        if let Some(lowest_persisted_partition) = lowest_persisted_partition {
            let partitions_names_with_queued_tasks = self
                .flush_manager
                .read()
                .expect("lock is poisoned")
                .get_partitions_with_tasks();

            if partitions_names_with_queued_tasks.contains(&lowest_persisted_partition.name) {
                return;
            }

            match lowest_persisted_partition.rotate_memtable() {
                Ok(_) => {}
                Err(e) => {
                    log::error!(
                        "monitor: memtable rotation failed for {:?}: {e:?}",
                        lowest_persisted_partition.name
                    );

                    self.keyspace_poison
                        .store(true, std::sync::atomic::Ordering::Release);
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

                    self.keyspace_poison
                        .store(true, std::sync::atomic::Ordering::Release);
                }
            }
        }
    }
}
