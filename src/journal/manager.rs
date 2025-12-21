// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::writer::Writer;
use crate::Keyspace;
use lsm_tree::{AbstractTree, SeqNo};
use std::{path::PathBuf, sync::MutexGuard};

/// Stores the highest seqno of a keyspace found in a journal.
#[derive(Clone)]
pub struct EvictionWatermark {
    pub(crate) keyspace: Keyspace,
    pub(crate) lsn: SeqNo,
}

impl std::fmt::Debug for EvictionWatermark {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.keyspace.name, self.lsn)
    }
}

pub struct Item {
    pub(crate) path: PathBuf,
    pub(crate) size_in_bytes: u64,
    pub(crate) watermarks: Vec<EvictionWatermark>,
}

impl std::fmt::Debug for Item {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "JournalManagerItem {:?} => {:#?}",
            self.path, self.watermarks
        )
    }
}

/// The [`JournalManager`] keeps track of sealed journals that are being flushed.
///
/// Each journal may contain items of different keyspaces.
#[expect(clippy::module_name_repetitions)]
#[derive(Debug)]
pub struct JournalManager {
    items: Vec<Item>,
    disk_space_in_bytes: u64,
}

impl Drop for JournalManager {
    fn drop(&mut self) {
        log::trace!("Dropping journal manager");

        #[cfg(feature = "__internal_whitebox")]
        crate::drop::decrement_drop_counter();
    }
}

impl JournalManager {
    pub(crate) fn new() -> Self {
        #[cfg(feature = "__internal_whitebox")]
        crate::drop::increment_drop_counter();

        Self {
            items: Vec::with_capacity(10),
            disk_space_in_bytes: 0,
        }
    }

    pub(crate) fn clear(&mut self) {
        self.items.clear();
    }

    pub(crate) fn enqueue(&mut self, item: Item) {
        self.disk_space_in_bytes = self.disk_space_in_bytes.saturating_add(item.size_in_bytes);
        self.items.push(item);
    }

    /// Returns the amount of journals
    pub(crate) fn journal_count(&self) -> usize {
        // NOTE: + 1 = active journal
        self.sealed_journal_count() + 1
    }

    /// Returns the amount of sealed journals
    pub(crate) fn sealed_journal_count(&self) -> usize {
        self.items.len()
    }

    /// Returns the amount of bytes used on disk by journals
    pub(crate) fn disk_space_used(&self) -> u64 {
        self.disk_space_in_bytes
    }

    /// Gets keyspaces to be flushed so that the oldest journal can be safely evicted
    pub(crate) fn get_keyspaces_to_flush_for_oldest_journal_eviction(&self) -> Vec<Keyspace> {
        let mut items = vec![];

        if let Some(item) = self.items.first() {
            for item in &item.watermarks {
                let Some(partition_seqno) = item.keyspace.tree.get_highest_persisted_seqno() else {
                    items.push(item.keyspace.clone());
                    continue;
                };

                if partition_seqno < item.lsn {
                    items.push(item.keyspace.clone());
                }
            }
        }

        items
    }

    /// Performs maintenance, maybe deleting some old journals
    pub(crate) fn maintenance(&mut self) -> crate::Result<()> {
        log::debug!("Running journal maintenance");

        loop {
            let Some(item) = self.items.first() else {
                return Ok(());
            };

            // TODO: unit test: check deleted keyspace does not prevent journal eviction
            for item in &item.watermarks {
                // Only check keyspace seqno if not deleted
                if !item
                    .keyspace
                    .is_deleted
                    .load(std::sync::atomic::Ordering::Acquire)
                {
                    let Some(keyspace_seqno) = item.keyspace.tree.get_highest_persisted_seqno()
                    else {
                        return Ok(());
                    };

                    if keyspace_seqno < item.lsn {
                        log::trace!(
                            "Keyspace {:?} not flushed enough to evict journal",
                            item.keyspace.name,
                        );
                        return Ok(());
                    }
                }
            }

            // NOTE: Once the LSN of *every* keyspace's tables [1] is higher than the journal's stored keyspace seqno,
            // it can be deleted from disk, as we know the entire journal has been flushed to tables [2].
            //
            // [1] We cannot use the keyspace's max seqno, because the memtable will get writes, which increase the seqno.
            // We *need* to check the tables specifically, they are the source of truth for flushed data.
            //
            // [2] Checking the seqno is safe because the queues inside the flush manager are FIFO.
            //
            // IMPORTANT: On recovery, the journals need to be flushed from oldest to newest.
            log::trace!("Removing fully flushed journal at {}", item.path.display());

            std::fs::remove_file(&item.path).inspect_err(|e| {
                log::error!(
                    "Failed to clean up stale journal file at {}: {e:?}",
                    item.path.display(),
                );
            })?;

            self.disk_space_in_bytes = self.disk_space_in_bytes.saturating_sub(item.size_in_bytes);
            self.items.remove(0);
        }
    }

    pub(crate) fn rotate_journal(
        &mut self,
        journal_writer: &mut MutexGuard<Writer>,
        watermarks: Vec<EvictionWatermark>,
    ) -> crate::Result<()> {
        let journal_size = journal_writer.len()?;

        let (sealed_path, _) = journal_writer.rotate()?;

        self.enqueue(Item {
            path: sealed_path,
            watermarks,
            size_in_bytes: journal_size,
        });

        Ok(())
    }
}
