// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    background_worker::Activity, config::Config as DatabaseConfig, db::Keyspaces,
    flush::manager::FlushManager, journal::manager::JournalManager,
    snapshot_tracker::SnapshotTracker, write_buffer_manager::WriteBufferManager, Database,
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
    pub(crate) db_config: DatabaseConfig,
    pub(crate) journal_manager: Arc<RwLock<JournalManager>>,
    pub(crate) write_buffer_manager: WriteBufferManager,
    pub(crate) keyspaces: Arc<RwLock<Keyspaces>>,
    pub(crate) seqno: SequenceNumberCounter,
    pub(crate) snapshot_tracker: SnapshotTracker,
    pub(crate) db_poison: Arc<AtomicBool>,
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

        let max_journal_size = self.db_config.max_journaling_size_in_bytes;

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
            let max_write_buffer_size = self.db_config.max_write_buffer_size_in_bytes;

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
    pub fn new(db: &Database) -> Self {
        #[cfg(feature = "__internal_whitebox")]
        crate::drop::increment_drop_counter();

        Self {
            flush_manager: db.flush_manager.clone(),
            journal_manager: db.journal_manager.clone(),
            db_config: db.config.clone(),
            write_buffer_manager: db.write_buffer_manager.clone(),
            keyspaces: db.keyspaces.clone(),
            seqno: db.seqno.clone(),
            snapshot_tracker: db.snapshot_tracker.clone(),
            db_poison: db.is_poisoned.clone(),
        }
    }

    fn try_reduce_journal_size(&self) {
        log::debug!(
            "monitor: try flushing affected keyspaces because journals have passed 50% of threshold"
        );

        let keyspaces = self.keyspaces.read().expect("lock is poisoned");

        // TODO: this may not scale well for many keyspaces
        let lowest_persisted_keyspace = keyspaces
            .values()
            .filter(|x| x.tree.active_memtable_size() > 0)
            .min_by(|a, b| {
                a.tree
                    .get_highest_persisted_seqno()
                    .cmp(&b.tree.get_highest_persisted_seqno())
            })
            .cloned();

        drop(keyspaces);

        if let Some(lowest_persisted_keyspace) = lowest_persisted_keyspace {
            let keyspaces_names_with_queued_tasks = self
                .flush_manager
                .read()
                .expect("lock is poisoned")
                .get_keyspaces_with_tasks();

            if keyspaces_names_with_queued_tasks.contains(&lowest_persisted_keyspace.name) {
                return;
            }

            match lowest_persisted_keyspace.rotate_memtable() {
                Ok(_) => {}
                Err(e) => {
                    log::error!(
                        "monitor: memtable rotation failed for {:?}: {e:?}",
                        lowest_persisted_keyspace.name
                    );

                    self.db_poison
                        .store(true, std::sync::atomic::Ordering::Release);
                }
            }
        }
    }

    fn try_reduce_write_buffer_size(&self) {
        log::trace!(
            "monitor: flush inactive keyspaces because write buffer has passed 50% of threshold"
        );

        let mut keyspaces = self
            .keyspaces
            .read()
            .expect("lock is poisoned")
            .values()
            .cloned()
            .collect::<Vec<_>>();

        keyspaces.sort_by(|a, b| {
            b.tree
                .active_memtable_size()
                .cmp(&a.tree.active_memtable_size())
        });

        let keyspaces_names_with_queued_tasks = self
            .flush_manager
            .read()
            .expect("lock is poisoned")
            .get_keyspaces_with_tasks();

        let keyspaces = keyspaces
            .into_iter()
            .filter(|x| !keyspaces_names_with_queued_tasks.contains(&x.name));

        for keyspace in keyspaces {
            log::debug!("monitor: WB rotating {:?}", keyspace.name);

            match keyspace.rotate_memtable() {
                Ok(rotated) => {
                    if rotated {
                        break;
                    }
                }
                Err(e) => {
                    log::error!(
                        "monitor: memtable rotation failed for {:?}: {e:?}",
                        keyspace.name
                    );

                    self.db_poison
                        .store(true, std::sync::atomic::Ordering::Release);
                }
            }
        }
    }
}
