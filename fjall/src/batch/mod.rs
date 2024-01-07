pub mod item;

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

pub use item::Item;
use lsm_tree::{Value, ValueType};

use crate::{Keyspace, PartitionHandle};

/// Partition key (a.k.a. column family, locality group)
pub type PartitionKey = Arc<str>;

/// An atomic write batch
///
/// Allows atomically writing across partitions inside the tree.
pub struct Batch {
    data: Vec<Item>,
    keyspace: Keyspace,
}

impl Batch {
    /// Initializes a new write batch
    /// This function is called by [`Keyspace::batch`]
    pub(crate) fn new(keyspace: Keyspace) -> Self {
        Self {
            data: Vec::with_capacity(100),
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

    /// Commits the batch to the LSM-tree atomically.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn commit(mut self) -> crate::Result<()> {
        let mut shard = self.keyspace.journal.get_writer();

        // NOTE: Fully (write) lock, so the batch can be committed atomically
        let partitions = self.keyspace.partitions.write().expect("lock is poisoned");

        // IMPORTANT: Need to WRITE lock all affected partition's memtables
        // Otherwise, there may be read skew
        let locked_memtables = {
            let mut lock_map = HashMap::new();

            for item in &self.data {
                let Some(partition) = partitions.get(&item.partition) else {
                    continue;
                };

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

        let mut partitions_with_possible_overflow = HashSet::new();

        log::trace!("Applying {} batched items to memtable(s)", self.data.len());
        for item in std::mem::take(&mut self.data) {
            let Some(partition) = partitions.get(&item.partition) else {
                continue;
            };

            let Some(lock) = locked_memtables.get(&item.partition) else {
                continue;
            };

            let value = Value {
                key: item.key,
                value: item.value,
                seqno: batch_seqno,
                value_type: item.value_type,
            };

            lock.insert(value);

            // IMPORTANT: Clone the handle, because we don't want to keep the partitions lock open
            partitions_with_possible_overflow.insert(partition.clone());
        }

        drop(locked_memtables);
        drop(partitions);
        drop(shard);

        for partition in partitions_with_possible_overflow {
            let memtable_size = partition.tree.active_memtable_size();
            if let Err(e) = partition.check_memtable_overflow(memtable_size) {
                log::error!("Failed memtable rotate check: {e:?}");
            };
        }

        Ok(())
    }
}
