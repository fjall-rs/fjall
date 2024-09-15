// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::writer::Writer;
use crate::PartitionHandle;
use lsm_tree::{AbstractTree, Memtable, SeqNo};
use std::{
    path::PathBuf,
    sync::{Arc, MutexGuard},
};

/// Stores the highest seqno of a partition found in a journal.
#[derive(Clone)]
pub struct EvictionWatermark {
    pub(crate) partition: PartitionHandle,
    pub(crate) lsn: SeqNo,
}

impl std::fmt::Debug for EvictionWatermark {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.partition.name, self.lsn)
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

// TODO: accessing journal manager shouldn't take RwLock... but changing its internals should

/// The [`JournalManager`] keeps track of sealed journals that are being flushed.
///
/// Each journal may contain items of different partitions.
#[allow(clippy::module_name_repetitions)]
#[derive(Debug)]
pub struct JournalManager {
    active_path: PathBuf, // TODO: remove?
    items: Vec<Item>,

    // TODO: should be taking into account active journal, which is preallocated...
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
    pub(crate) fn from_active<P: Into<PathBuf>>(path: P) -> Self {
        #[cfg(feature = "__internal_whitebox")]
        crate::drop::increment_drop_counter();

        Self {
            active_path: path.into(),
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

    /// Rotates & lists partitions to be flushed so that the oldest journal can be safely evicted
    pub(crate) fn rotate_partitions_to_flush_for_oldest_journal_eviction(
        &self,
    ) -> Vec<(PartitionHandle, EvictionWatermark, u64, Arc<Memtable>)> {
        let mut v = Vec::new();

        if let Some(item) = self.items.first() {
            for item in &item.watermarks {
                let highest_persisted_seqno = item.partition.tree.get_highest_persisted_seqno();

                if highest_persisted_seqno.is_none()
                    || highest_persisted_seqno.expect("unwrap") < item.lsn
                {
                    if let Some((yanked_id, yanked_memtable)) =
                        item.partition.tree.rotate_memtable()
                    {
                        v.push((
                            item.partition.clone(),
                            EvictionWatermark {
                                partition: item.partition.clone(),
                                lsn: yanked_memtable
                                    .get_highest_seqno()
                                    .expect("memtable should not be empty"),
                            },
                            yanked_id,
                            yanked_memtable,
                        ));
                    }
                }
            }
        }

        v
    }

    /// Performs maintenance, maybe deleting some old journals
    pub(crate) fn maintenance(&mut self) -> crate::Result<()> {
        loop {
            let Some(item) = self.items.first() else {
                return Ok(());
            };

            // TODO: unit test: check deleted partition does not prevent journal eviction
            for item in &item.watermarks {
                // Only check partition seqno if not deleted
                if !item
                    .partition
                    .is_deleted
                    .load(std::sync::atomic::Ordering::Acquire)
                {
                    let Some(partition_seqno) = item.partition.tree.get_highest_persisted_seqno()
                    else {
                        return Ok(());
                    };

                    if partition_seqno < item.lsn {
                        return Ok(());
                    }
                }
            }

            // NOTE: Once the LSN of *every* partition's segments [1] is higher than the journal's stored partition seqno,
            // it can be deleted from disk, as we know the entire journal has been flushed to segments [2].
            //
            // [1] We cannot use the partition's max seqno, because the memtable will get writes, which increase the seqno.
            // We *need* to check the disk segments specifically, they are the source of truth for flushed data.
            //
            // [2] Checking the seqno is safe because the queues inside the flush manager are FIFO.
            //
            // IMPORTANT: On recovery, the journals need to be flushed from oldest to newest.
            log::trace!("Removing fully flushed journal at {:?}", item.path);
            std::fs::remove_file(&item.path)?;

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

        let (sealed_path, next_journal_path) = journal_writer.rotate()?;
        self.active_path = next_journal_path;

        self.enqueue(Item {
            path: sealed_path,
            watermarks,
            size_in_bytes: journal_size,
        });

        Ok(())
    }
}
