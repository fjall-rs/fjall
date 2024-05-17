use self::partition::TxPartitionHandle;
use crate::{batch::PartitionKey, Instant};
use lsm_tree::{MemTable, SeqNo, UserKey, UserValue, Value};
use std::{
    collections::HashMap,
    ops::RangeBounds,
    sync::{Arc, MutexGuard},
};

pub mod keyspace;
pub mod partition;

fn ignore_tombstone_value(item: Value) -> Option<Value> {
    if item.is_tombstone() {
        None
    } else {
        Some(item)
    }
}

/// A serializable cross-partition transaction.
///
/// Use [`Transaction::commit`] to commit changes to the partition(s).
///
/// Drop the transaction to rollback changes.
pub struct Transaction<'a> {
    memtables: HashMap<PartitionKey, Arc<MemTable>>,
    instant: Instant,
    tx_lock: MutexGuard<'a, ()>,
}

impl<'a> Transaction<'a> {
    pub(crate) fn new(tx_lock: MutexGuard<'a, ()>, instant: Instant) -> Self {
        Self {
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

    /// Iterate over the transaction's state
    #[must_use]
    pub fn iter<'b>(
        &'b mut self,
        partition: &'b TxPartitionHandle,
    ) -> impl DoubleEndedIterator<Item = crate::Result<(UserKey, UserValue)>> + 'b {
        partition
            .inner
            .tree
            .create_iter(
                Some(self.instant),
                Some(
                    self.memtables
                        .entry(partition.inner.name.clone())
                        .or_default(),
                ),
            )
            .map(|item| Ok(item?))
    }

    /// Iterate over a range of the transaction's state
    #[must_use]
    pub fn range<'b, K: AsRef<[u8]>, R: RangeBounds<K>>(
        &'b mut self,
        partition: &'b TxPartitionHandle,
        range: R,
    ) -> impl DoubleEndedIterator<Item = crate::Result<(UserKey, UserValue)>> + 'b {
        partition
            .inner
            .tree
            .create_range(
                range,
                Some(self.instant),
                Some(
                    self.memtables
                        .entry(partition.inner.name.clone())
                        .or_default(),
                ),
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
                self.instant - 1, // TODO: if the seqno is too high, the merge iterator will hide it... the seqno should only apply to tree items, not the tx read set
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
                // TODO: need an internal SeqNo..., then on commit only get the newest items and use
                // the transaction's instant
                SeqNo::MAX,
            ));
    }

    // TODO: conflict

    /// Commits the transaction
    pub fn commit(self) -> crate::Result<Result<(), ()>> {
        todo!();

        // TODO: rewrite seqnos
        // TODO: lock partitions
        // TODO: write items
        // TODO: check write buffer stuff, see batch

        // TODO: persist?

        Ok(Ok(()))
    }

    /// More explicit alternative to dropping the transaction
    /// to roll it back.
    pub fn rollback(self) {}
}
