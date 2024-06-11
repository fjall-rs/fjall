pub mod item;

use crate::{Keyspace, PartitionHandle};
use item::Item;
use lsm_tree::{AbstractTree, ValueType};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

/// Partition key (a.k.a. column family, locality group)
pub type PartitionKey = Arc<str>;

/// An atomic write batch
///
/// Allows atomically writing across partitions inside the [`Keyspace`].
pub struct Batch {
    pub(crate) data: Vec<Item>,
    keyspace: Keyspace,
}

impl Batch {
    /*  /// Initializes a new write batch.
    ///
    /// This function is called by [`Keyspace::batch`].
    pub(crate) fn new(keyspace: Keyspace) -> Self {
        Self::with_capacity(keyspace, 0)
    } */

    /// Initializes a new write batch with at least 'N' items capacity.
    pub(crate) fn with_capacity(keyspace: Keyspace, capacity: usize) -> Self {
        Self {
            data: Vec::with_capacity(capacity),
            keyspace,
        }
    }

    /// Inserts a key-value pair into the batch
    pub fn insert<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        &mut self,
        p: &PartitionHandle,
        key: K,
        value: V,
    ) {
        self.data.push(Item::new(
            p.name.clone(),
            key.as_ref(),
            value.as_ref(),
            ValueType::Value,
        ));
    }

    /// Adds a tombstone marker for a key
    pub fn remove<K: AsRef<[u8]>>(&mut self, p: &PartitionHandle, key: K) {
        self.data.push(Item::new(
            p.name.clone(),
            key.as_ref(),
            vec![],
            ValueType::Tombstone,
        ));
    }

    /// Commits the batch to the [`Keyspace`] atomically
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn commit(mut self) -> crate::Result<()> {
        if self
            .keyspace
            .is_poisoned
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            return Err(crate::Error::Poisoned);
        }

        log::trace!("batch: Acquiring shard");
        let mut shard = self.keyspace.journal.get_writer();

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

                if partition
                    .is_deleted
                    .load(std::sync::atomic::Ordering::Relaxed)
                {
                    return Err(crate::Error::PartitionDeleted);
                }

                lock_map.insert(
                    item.partition.clone(),
                    partition.tree.lock_active_memtable(),
                );
            }

            lock_map
        };

        let batch_seqno = self.keyspace.seqno.next();

        let items = self.data.iter().collect::<Vec<_>>();
        let _ = shard.writer.write_batch(&items, batch_seqno)?;

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

        drop(locked_memtables);
        drop(partitions);
        drop(shard);

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
