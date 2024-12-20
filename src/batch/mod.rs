// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

pub mod item;

use crate::{Keyspace, PartitionHandle, PersistMode};
use item::Item;
use lsm_tree::{AbstractTree, UserKey, UserValue, ValueType};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

/// Partition key (a.k.a. column family, locality group)
pub type PartitionKey = Arc<str>;

/// An atomic write batch
///
/// Allows atomically writing across partitions inside the [`Keyspace`].
#[doc(alias = "WriteBatch")]
pub struct Batch {
    pub(crate) data: Vec<Item>,
    keyspace: Keyspace,
    durability: Option<PersistMode>,
}

impl Batch {
    /// Initializes a new write batch.
    ///
    /// This function is called by [`Keyspace::batch`].
    pub(crate) fn new(keyspace: Keyspace) -> Self {
        Self {
            data: Vec::new(),
            keyspace,
            durability: None,
        }
    }

    /// Initializes a new write batch with preallocated capacity.
    #[must_use]
    pub fn with_capacity(keyspace: Keyspace, capacity: usize) -> Self {
        Self {
            data: Vec::with_capacity(capacity),
            keyspace,
            durability: None,
        }
    }

    /// Sets the durability level.
    #[must_use]
    pub fn durability(mut self, mode: Option<PersistMode>) -> Self {
        self.durability = mode;
        self
    }

    /// Inserts a key-value pair into the batch
    pub fn insert<K: Into<UserKey>, V: Into<UserValue>>(
        &mut self,
        p: &PartitionHandle,
        key: K,
        value: V,
    ) {
        self.data
            .push(Item::new(p.name.clone(), key, value, ValueType::Value));
    }

    /// Adds a tombstone marker for a key
    pub fn remove<K: Into<UserKey>>(&mut self, p: &PartitionHandle, key: K) {
        self.data
            .push(Item::new(p.name.clone(), key, vec![], ValueType::Tombstone));
    }

    /// Commits the batch to the [`Keyspace`] atomically
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn commit(mut self) -> crate::Result<()> {
        use std::sync::atomic::Ordering;

        log::trace!("batch: Acquiring journal writer");
        let mut journal_writer = self.keyspace.journal.get_writer();

        // IMPORTANT: Check the poisoned flag after getting journal mutex, otherwise TOCTOU
        if self.keyspace.is_poisoned.load(Ordering::Relaxed) {
            return Err(crate::Error::Poisoned);
        }

        let batch_seqno = self.keyspace.seqno.next();

        let _ = journal_writer.write_batch(self.data.iter(), self.data.len(), batch_seqno);

        if let Some(mode) = self.durability {
            if let Err(e) = journal_writer.persist(mode) {
                self.keyspace.is_poisoned.store(true, Ordering::Release);

                log::error!(
                    "persist failed, which is a FATAL, and possibly hardware-related, failure: {e:?}"
                );

                return Err(crate::Error::Poisoned);
            }
        }

        // NOTE: Fully (write) lock, so the batch can be committed atomically
        log::trace!("batch: Acquiring partitions lock");
        let partitions = self.keyspace.partitions.write().expect("lock is poisoned");

        // IMPORTANT: Need to WRITE lock all affected partition's memtables
        // Otherwise, there may be read skew
        log::trace!("batch: Acquiring memtable locks");
        let locked_memtables = {
            let mut lock_map = HashMap::new();

            for item in &self.data {
                if lock_map.contains_key(&item.partition) {
                    continue;
                }

                let Some(partition) = partitions.get(&item.partition) else {
                    continue;
                };

                if partition.is_deleted.load(Ordering::Relaxed) {
                    return Err(crate::Error::PartitionDeleted);
                }

                lock_map.insert(
                    item.partition.clone(),
                    partition.tree.lock_active_memtable(),
                );
            }

            lock_map
        };

        #[allow(clippy::mutable_key_type)]
        let mut partitions_with_possible_stall = HashSet::new();

        let mut batch_size = 0u64;

        log::trace!("Applying {} batched items to memtable(s)", self.data.len());
        for item in std::mem::take(&mut self.data) {
            let Some(partition) = partitions.get(&item.partition) else {
                continue;
            };

            let Some(active_memtable) = locked_memtables.get(&item.partition) else {
                continue;
            };

            let (item_size, _) = partition.tree.raw_insert_with_lock(
                active_memtable,
                item.key,
                item.value,
                batch_seqno,
                item.value_type,
            );

            batch_size += u64::from(item_size);

            // IMPORTANT: Clone the handle, because we don't want to keep the partitions lock open
            partitions_with_possible_stall.insert(partition.clone());
        }

        drop(journal_writer);

        drop(locked_memtables);
        drop(partitions);

        // IMPORTANT: Add batch size to current write buffer size
        // Otherwise write buffer growth is unbounded when using batches
        self.keyspace.write_buffer_manager.allocate(batch_size);

        // Check each affected partition for write stall/halt
        for partition in partitions_with_possible_stall {
            let memtable_size = partition.tree.active_memtable_size();

            if let Err(e) = partition.check_memtable_overflow(memtable_size) {
                log::error!("Failed memtable rotate check: {e:?}");
            };

            // IMPORTANT: Check write buffer as well
            // Otherwise batch writes are never stalled/halted
            let write_buffer_size = self.keyspace.write_buffer_manager.get();
            partition.check_write_buffer_size(write_buffer_size);
        }

        Ok(())
    }
}
