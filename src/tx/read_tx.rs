use crate::{Instant, TxPartitionHandle};
use lsm_tree::{UserKey, UserValue};
use std::ops::RangeBounds;

/// A cross-partition, read-only transaction (snapshot).
pub struct ReadTransaction {
    instant: Instant,
}

impl ReadTransaction {
    pub(crate) fn new(instant: Instant) -> Self {
        Self { instant }
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
    ) -> crate::Result<Option<UserValue>> {
        Ok(partition.inner.snapshot_at(self.instant).get(key)?)
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
            .create_iter(Some(self.instant), None)
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
            .create_range(range, Some(self.instant), None)
            .map(|item| Ok(item?))
    }

    /// Iterate over a range of the transaction's state
    #[must_use]
    pub fn prefix<'b, K: AsRef<[u8]>>(
        &'b mut self,
        partition: &'b TxPartitionHandle,
        prefix: K,
    ) -> impl DoubleEndedIterator<Item = crate::Result<(UserKey, UserValue)>> + 'b {
        partition
            .inner
            .tree
            .create_prefix(prefix, Some(self.instant), None)
            .map(|item| Ok(item?))
    }
}
