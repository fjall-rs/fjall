use crate::PartitionHandle;
use std::sync::{Arc, Mutex};

/// Access to a partition of a transactional keyspace
pub struct TransactionalPartitionHandle {
    pub(crate) inner: PartitionHandle,
    pub(crate) tx_lock: Arc<Mutex<()>>,
}

/// Alias for [`TransactionalPartitionHandle`]
pub type TxPartitionHandle = TransactionalPartitionHandle;

impl TxPartitionHandle {
    /// Inserts a key-value pair into the partition.
    ///
    /// The operation will be run wrapped in a transaction.
    ///
    /// Keys may be up to 65536 bytes long, values up to 2^32 bytes.
    /// Shorter keys and values result in better performance.
    ///
    /// If the key already exists, the item will be overwritten.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn insert<K: AsRef<[u8]>, V: AsRef<[u8]>>(&self, key: K, value: V) -> crate::Result<()> {
        let _lock = self.tx_lock.lock().expect("lock is poisoned");
        self.inner.insert(key, value)
    }

    /// Removes an item from the partition.
    ///
    /// The operation will be run wrapped in a transaction.
    ///
    /// The key may be up to 65536 bytes long.
    /// Shorter keys result in better performance.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn remove<K: AsRef<[u8]>>(&self, key: K) -> crate::Result<()> {
        let _lock = self.tx_lock.lock().expect("lock is poisoned");
        self.inner.remove(key)
    }

    /// Retrieves an item from the partition.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> crate::Result<Option<lsm_tree::UserValue>> {
        self.inner.get(key)
    }

    /// Allows access to the inner partition handle, allowing to
    /// escape from the transactional context.
    #[must_use]
    pub fn inner(&self) -> &PartitionHandle {
        &self.inner
    }

    // TODO: snapshot (read_tx?)
}
