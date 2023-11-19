use log::trace;

use crate::{Tree, Value};

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
    pub fn insert<K: Into<Vec<u8>>, V: Into<Vec<u8>>>(&mut self, key: K, value: V) {
        self.data.push(Value::new(
            key.into(),
            value.into(),
            false,
            0, /* TODO: */
        ));
    }

    /// Adds a tombstone marker for a key
    pub fn remove<K: Into<Vec<u8>>>(&mut self, key: K) {
        self.data
            .push(Value::new(key.into(), vec![], true, 0 /* TODO: */));
    }

    /// Commits the batch to the LSM-tree atomically
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs
    pub fn commit(mut self) -> crate::Result<()> {
        /* let mut commit_log = self.tree.commit_log.lock().expect("lock poisoned");
        let mut memtable = self.tree.active_memtable.write().expect("lock poisoned"); */

        let memtable = &self.tree.active_memtable;
        let commit_log = &self.tree.commit_log;

        commit_log.append_batch(self.data.clone())?;
        commit_log.flush()?;

        trace!("Applying {} batched items to memtable", self.data.len());
        for entry in std::mem::take(&mut self.data) {
            memtable.insert(entry, 0 /* TODO: */);
        }

        /* let is_memtable_full = memtable.exceeds_threshold(self.tree.config.max_memtable_size);
        drop(memtable); */

        /*         if is_memtable_full {
            self.tree.start_flush_thread()?;
        } */

        Ok(())
    }
}
