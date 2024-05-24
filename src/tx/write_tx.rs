use crate::{
    batch::{item::Item, PartitionKey},
    Batch, Instant, Keyspace, TxPartitionHandle,
};
use lsm_tree::{MemTable, SeqNo, UserKey, UserValue, Value};
use std::{
    collections::HashMap,
    ops::{Deref, RangeBounds},
    sync::{Arc, MutexGuard},
};

fn ignore_tombstone_value(item: Value) -> Option<Value> {
    if item.is_tombstone() {
        None
    } else {
        Some(item)
    }
}

/// A single-writer (serialized) cross-partition transaction.
///
/// Use [`WriteTransaction::commit`] to commit changes to the partition(s).
///
/// Drop the transaction to rollback changes.
pub struct WriteTransaction<'a> {
    keyspace: Keyspace,
    memtables: HashMap<PartitionKey, Arc<MemTable>>,
    instant: Instant,

    #[allow(unused)]
    tx_lock: MutexGuard<'a, ()>,
}

impl<'a> WriteTransaction<'a> {
    pub(crate) fn new(keyspace: Keyspace, tx_lock: MutexGuard<'a, ()>, instant: Instant) -> Self {
        Self {
            keyspace,
            memtables: HashMap::default(),
            instant,
            tx_lock,
        }
    }

    /// Retrieves an item from the partition.
    ///
    /// The transaction allows reading your own writes (RYOW).
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open_tx()?;
    /// # let partition = keyspace.open_partition("default", PartitionCreateOptions::default())?;
    /// partition.insert("a", "previous_value")?;
    /// assert_eq!(b"previous_value", &*partition.get("a")?.unwrap());
    ///
    /// let mut tx = keyspace.tx();
    /// tx.insert(&partition, "a", "new_value");
    ///
    /// // Read-your-own-write
    /// let item = tx.get(&partition, "a")?;
    /// assert_eq!(Some("new_value".as_bytes().into()), item);
    ///
    /// drop(tx);
    ///
    /// // Write was not committed
    /// assert_eq!(b"previous_value", &*partition.get("a")?.unwrap());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn get<K: AsRef<[u8]>>(
        &self,
        partition: &TxPartitionHandle,
        key: K,
    ) -> crate::Result<Option<lsm_tree::UserValue>> {
        if let Some(memtable) = self.memtables.get(&partition.inner.name) {
            if let Some(item) = memtable.get(&key, None) {
                return Ok(ignore_tombstone_value(item).map(|x| x.value));
            }
        }

        partition.inner.get(key)
    }

    /// Returns the first key-value pair in the transaction's state.
    /// The key in this pair is the minimum key in the transaction's state.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn first_key_value(
        &self,
        partition: &TxPartitionHandle,
    ) -> crate::Result<Option<(UserKey, UserValue)>> {
        self.iter(partition).next().transpose()
    }

    /// Returns the last key-value pair in the transaction's state.
    /// The key in this pair is the maximum key in the transaction's state.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn last_key_value(
        &self,
        partition: &TxPartitionHandle,
    ) -> crate::Result<Option<(UserKey, UserValue)>> {
        self.iter(partition).next_back().transpose()
    }

    /// Iterate over the transaction's state
    #[must_use]
    pub fn iter<'b>(
        &'b self,
        partition: &'b TxPartitionHandle,
    ) -> impl DoubleEndedIterator<Item = crate::Result<(UserKey, UserValue)>> + 'b {
        partition
            .inner
            .tree
            .create_iter(
                Some(self.instant),
                self.memtables.get(&partition.inner.name).map(Deref::deref),
            )
            .map(|item| Ok(item?))
    }

    /// Iterate over a range of the transaction's state
    #[must_use]
    pub fn range<'b, K: AsRef<[u8]>, R: RangeBounds<K>>(
        &'b self,
        partition: &'b TxPartitionHandle,
        range: R,
    ) -> impl DoubleEndedIterator<Item = crate::Result<(UserKey, UserValue)>> + 'b {
        partition
            .inner
            .tree
            .create_range(
                range,
                Some(self.instant),
                self.memtables.get(&partition.inner.name).map(Deref::deref),
            )
            .map(|item| Ok(item?))
    }

    /// Iterate over a range of the transaction's state
    #[must_use]
    pub fn prefix<'b, K: AsRef<[u8]>>(
        &'b self,
        partition: &'b TxPartitionHandle,
        prefix: K,
    ) -> impl DoubleEndedIterator<Item = crate::Result<(UserKey, UserValue)>> + 'b {
        partition
            .inner
            .tree
            .create_prefix(
                prefix,
                Some(self.instant),
                self.memtables.get(&partition.inner.name).map(Deref::deref),
            )
            .map(|item| Ok(item?))
    }

    /// Inserts a key-value pair into the partition.
    ///
    /// Keys may be up to 65536 bytes long, values up to 2^32 bytes.
    /// Shorter keys and values result in better performance.
    ///
    /// If the key already exists, the item will be overwritten.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open_tx()?;
    /// # let partition = keyspace.open_partition("default", PartitionCreateOptions::default())?;
    /// partition.insert("a", "previous_value")?;
    /// assert_eq!(b"previous_value", &*partition.get("a")?.unwrap());
    ///
    /// let mut tx = keyspace.tx();
    /// tx.insert(&partition, "a", "new_value");
    ///
    /// drop(tx);
    ///
    /// // Write was not committed
    /// assert_eq!(b"previous_value", &*partition.get("a")?.unwrap());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn insert<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        &mut self,
        partition: &TxPartitionHandle,
        key: K,
        value: V,
    ) {
        self.memtables
            .entry(partition.inner.name.clone())
            .or_default()
            .insert(lsm_tree::Value::new(
                key.as_ref(),
                value.as_ref(),
                // NOTE: Just take the max seqno, which should never be reached
                // that way, the write is definitely always the newest
                SeqNo::MAX,
                lsm_tree::ValueType::Value,
            ));
    }

    /// Removes an item from the partition.
    ///
    /// The key may be up to 65536 bytes long.
    /// Shorter keys result in better performance.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open_tx()?;
    /// # let partition = keyspace.open_partition("default", PartitionCreateOptions::default())?;
    /// partition.insert("a", "previous_value")?;
    /// assert_eq!(b"previous_value", &*partition.get("a")?.unwrap());
    ///
    /// let mut tx = keyspace.tx();
    /// tx.remove(&partition, "a");
    ///
    /// // Read-your-own-write
    /// let item = tx.get(&partition, "a")?;
    /// assert_eq!(None, item);
    ///
    /// drop(tx);
    ///
    /// // Deletion was not committed
    /// assert_eq!(b"previous_value", &*partition.get("a")?.unwrap());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn remove<K: AsRef<[u8]>>(&mut self, partition: &TxPartitionHandle, key: K) {
        self.memtables
            .entry(partition.inner.name.clone())
            .or_default()
            .insert(lsm_tree::Value::new_tombstone(
                key.as_ref(),
                // NOTE: Just take the max seqno, which should never be reached
                // that way, the write is definitely always the newest
                SeqNo::MAX,
            ));
    }

    /// Commits the transaction.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn commit(self) -> crate::Result<()> {
        let mut batch = Batch::with_capacity(self.keyspace, 10);

        for (partition_key, memtable) in &self.memtables {
            for entry in &memtable.items {
                let key = entry.key();
                let value = entry.value();

                batch.data.push(Item::new(
                    partition_key.clone(),
                    key.user_key.clone(),
                    value.clone(),
                    key.value_type,
                ));
            }
        }

        // TODO: instead of using batch, write batch::commit as a generic function that takes
        // a impl Iterator<BatchItem>
        batch.commit()
    }

    /// More explicit alternative to dropping the transaction
    /// to roll it back.
    pub fn rollback(self) {}
}
