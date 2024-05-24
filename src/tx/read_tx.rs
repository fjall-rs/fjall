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

    /// Retrieves an item from the transaction's state.
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

    /// Returns `true` if the transaction's state contains the specified key.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn contains_key<K: AsRef<[u8]>>(
        &self,
        partition: &TxPartitionHandle,
        key: K,
    ) -> crate::Result<bool> {
        self.get(partition, key).map(|x| x.is_some())
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

    /// Iterates over the transaction's state
    #[must_use]
    pub fn iter<'a>(
        &'a self,
        partition: &'a TxPartitionHandle,
    ) -> impl DoubleEndedIterator<Item = crate::Result<(UserKey, UserValue)>> + 'a {
        partition
            .inner
            .tree
            .create_iter(Some(self.instant), None)
            .map(|item| Ok(item?))
    }

    /// Iterates over a range of the transaction's state
    #[must_use]
    pub fn range<'a, K: AsRef<[u8]>, R: RangeBounds<K>>(
        &'a mut self,
        partition: &'a TxPartitionHandle,
        range: R,
    ) -> impl DoubleEndedIterator<Item = crate::Result<(UserKey, UserValue)>> + 'a {
        partition
            .inner
            .tree
            .create_range(range, Some(self.instant), None)
            .map(|item| Ok(item?))
    }

    /// Iterates over a range of the transaction's state
    #[must_use]
    pub fn prefix<'a, K: AsRef<[u8]>>(
        &'a mut self,
        partition: &'a TxPartitionHandle,
        prefix: K,
    ) -> impl DoubleEndedIterator<Item = crate::Result<(UserKey, UserValue)>> + 'a {
        partition
            .inner
            .tree
            .create_prefix(prefix, Some(self.instant), None)
            .map(|item| Ok(item?))
    }
}
