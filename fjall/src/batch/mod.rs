pub(crate) mod item;

use std::sync::Arc;

pub use item::Item;
use lsm_tree::Tree;

/// Partition key (a.k.a. column family, locality group)
pub type PartitionKey = Arc<str>;

/// An atomic write batch.
///
/// Allows atomically writing across partitions inside the tree.
pub struct Batch {
    data: Vec<Item>,
    tree: Tree,
}

impl Batch {
    /// Initializes a new write batch
    /// This function is called by Tree.batch()
    pub(crate) fn new(tree: Tree) -> Self {
        Self {
            data: Vec::with_capacity(100),
            tree,
        }
    }

    /* /// Inserts a key-value pair into the batch
    pub fn insert<K: AsRef<[u8]>, V: AsRef<[u8]>>(&mut self, key: K, value: V) {
        self.data.push(BatchItem::new(
            get_default_partition_key(),
            key.as_ref(),
            value.as_ref(),
            ValueType::Value,
        ));
    } */

    /* /// Adds a tombstone marker for a key
    pub fn remove<K: AsRef<[u8]>>(&mut self, key: K) {
        self.data.push(BatchItem::new(
            get_default_partition_key(),
            key.as_ref(),
            vec![],
            ValueType::Tombstone,
        ));
    } */

    /// Commits the batch to the LSM-tree atomically.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn commit(mut self) -> crate::Result<()> {
        // TODO: 0.3.0 batch support partitions

        // TODO: 0.3.0
        /* let mut shard = self.tree.journal.lock_shard();

        // NOTE: Fully (write) lock, so the batch can be committed atomically
        let memtable_lock = self
            .tree
            .get_default_partition()
            .active_memtable
            .write()
            .expect("lock is poisoned");

        let batch_seqno = self.tree.increment_lsn();

        for item in &mut self.data {
            item.seqno = batch_seqno;
        }

        let items = self.data.iter().collect::<Vec<_>>();
        let _ = shard.write_batch(&items)?;
        shard.flush()?;

        /*  // NOTE: Add some pointers to better approximate memory usage of memtable
        // Because the data is stored with less overhead than in memory
        let size = bytes_written_to_disk
            + (items.len() * (std::mem::size_of::<UserKey>() + std::mem::size_of::<UserValue>())); */

        log::trace!("Applying {} batched items to memtable", self.data.len());
        for entry in std::mem::take(&mut self.data) {
            // TODO: From<BatchItem> for Value maybe
            let table = memtable_lock
                .items
                .get(&entry.partition)
                .expect("partition should exist");

            let size = entry.partition.len()
                + entry.key.len()
                + entry.value.len()
                + std::mem::size_of::<BatchItem>();

            table.insert(Value {
                key: entry.key,
                value: entry.value,
                seqno: entry.seqno,
                value_type: entry.value_type,
            });

            table
                .approximate_size
                .fetch_add(size as u32, std::sync::atomic::Ordering::AcqRel);
        }

        drop(memtable_lock);
        drop(shard);

        // TODO: 0.3.0 handle memtable too large
        /* if memtable_size > self.tree.config.max_memtable_size {
            log::debug!("Memtable reached threshold size");
            crate::flush::start(&self.tree)?;
        } */ */

        Ok(())
    }
}
