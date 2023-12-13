use crate::{value::ValueType, Tree, Value};

/// An atomic write batch
pub struct Batch {
    data: Vec<Value>,
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

    /// Inserts a key-value pair into the batch
    pub fn insert<K: AsRef<[u8]>, V: AsRef<[u8]>>(&mut self, key: K, value: V) {
        self.data.push(Value::new(
            key.as_ref(),
            value.as_ref(),
            0,
            ValueType::Value,
        ));
    }

    /// Adds a tombstone marker for a key
    pub fn remove<K: AsRef<[u8]>>(&mut self, key: K) {
        self.data
            .push(Value::new(key.as_ref(), vec![], 0, ValueType::Tombstone));
    }

    /// Commits the batch to the LSM-tree atomically.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn commit(mut self) -> crate::Result<()> {
        let mut shard = self.tree.journal.lock_shard();

        // NOTE: Fully (write) lock, so the batch can be committed atomically
        let memtable_lock = self.tree.active_memtable.write().expect("lock is poisoned");

        let batch_seqno = self.tree.increment_lsn();

        for item in &mut self.data {
            item.seqno = batch_seqno;
        }

        let items = self.data.iter().collect::<Vec<_>>();
        let bytes_written_to_disk = shard.write_batch(&items)?;
        shard.flush()?;

        // NOTE: Add some pointers to better approximate memory usage of memtable
        // Because the data is stored with less overhead than in memory
        let size = bytes_written_to_disk + (items.len() * std::mem::size_of::<Vec<u8>>() * 2);
        let memtable_size = self
            .tree
            .approx_active_memtable_size
            .fetch_add(size as u32, std::sync::atomic::Ordering::AcqRel);

        log::trace!("Applying {} batched items to memtable", self.data.len());
        for entry in std::mem::take(&mut self.data) {
            memtable_lock.insert(entry);
        }

        drop(memtable_lock);
        drop(shard);

        if memtable_size > self.tree.config.max_memtable_size {
            log::debug!("Memtable reached threshold size");
            crate::flush::start(&self.tree)?;
        }

        Ok(())
    }
}
