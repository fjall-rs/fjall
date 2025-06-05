// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{snapshot_nonce::SnapshotNonce, TxPartitionHandle};
use lsm_tree::{AbstractTree, KvPair, UserKey, UserValue};
use std::ops::RangeBounds;

/// A cross-partition, read-only transaction (snapshot)
pub struct ReadTransaction {
    nonce: SnapshotNonce,
}

impl ReadTransaction {
    pub(crate) fn new(nonce: SnapshotNonce) -> Self {
        Self { nonce }
    }

    /// Retrieves an item from the transaction's state.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open_transactional()?;
    /// # let partition = keyspace.open_partition("default", PartitionCreateOptions::default())?;
    /// partition.insert("a", "my_value")?;
    ///
    /// let tx = keyspace.read_tx();
    /// let item = tx.get(&partition, "a")?;
    /// assert_eq!(Some("my_value".as_bytes().into()), item);
    ///
    /// partition.insert("b", "my_updated_value")?;
    ///
    /// // Repeatable read
    /// let item = tx.get(&partition, "a")?;
    /// assert_eq!(Some("my_value".as_bytes().into()), item);
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
        partition
            .inner
            .tree
            .snapshot_at(self.nonce.instant)
            .get(key)
            .map_err(Into::into)
    }

    /// Retrieves the size of an item from the transaction's state.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open_transactional()?;
    /// # let partition = keyspace.open_partition("default", PartitionCreateOptions::default())?;
    /// partition.insert("a", "my_value")?;
    ///
    /// let tx = keyspace.read_tx();
    /// let item = tx.size_of(&partition, "a")?.unwrap_or_default();
    /// assert_eq!("my_value".len() as u32, item);
    ///
    /// partition.insert("b", "my_updated_value")?;
    ///
    /// // Repeatable read
    /// let item = tx.size_of(&partition, "a")?.unwrap_or_default();
    /// assert_eq!("my_value".len() as u32, item);
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn size_of<K: AsRef<[u8]>>(
        &self,
        partition: &TxPartitionHandle,
        key: K,
    ) -> crate::Result<Option<u32>> {
        partition
            .inner
            .tree
            .snapshot_at(self.nonce.instant)
            .size_of(key)
            .map_err(Into::into)
    }

    /// Returns `true` if the transaction's state contains the specified key.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open_transactional()?;
    /// # let partition = keyspace.open_partition("default", PartitionCreateOptions::default())?;
    /// partition.insert("a", "my_value")?;
    ///
    /// let tx = keyspace.read_tx();
    /// assert!(tx.contains_key(&partition, "a")?);
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn contains_key<K: AsRef<[u8]>>(
        &self,
        partition: &TxPartitionHandle,
        key: K,
    ) -> crate::Result<bool> {
        partition
            .inner
            .tree
            .snapshot_at(self.nonce.instant)
            .contains_key(key)
            .map_err(Into::into)
    }

    /// Returns the first key-value pair in the transaction's state.
    /// The key in this pair is the minimum key in the transaction's state.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open_transactional()?;
    /// # let partition = keyspace.open_partition("default", PartitionCreateOptions::default())?;
    /// partition.insert("1", "abc")?;
    /// partition.insert("3", "abc")?;
    /// partition.insert("5", "abc")?;
    ///
    /// let (key, _) = keyspace.read_tx().first_key_value(&partition)?.expect("item should exist");
    /// assert_eq!(&*key, "1".as_bytes());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn first_key_value(&self, partition: &TxPartitionHandle) -> crate::Result<Option<KvPair>> {
        self.iter(partition).next().transpose()
    }

    /// Returns the last key-value pair in the transaction's state.
    /// The key in this pair is the maximum key in the transaction's state.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open_transactional()?;
    /// # let partition = keyspace.open_partition("default", PartitionCreateOptions::default())?;
    /// partition.insert("1", "abc")?;
    /// partition.insert("3", "abc")?;
    /// partition.insert("5", "abc")?;
    ///
    /// let (key, _) = keyspace.read_tx().last_key_value(&partition)?.expect("item should exist");
    /// assert_eq!(&*key, "5".as_bytes());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn last_key_value(&self, partition: &TxPartitionHandle) -> crate::Result<Option<KvPair>> {
        self.iter(partition).next_back().transpose()
    }

    /// Scans the entire partition, returning the amount of items.
    ///
    /// ###### Caution
    ///
    /// This operation scans the entire partition: O(n) complexity!
    ///
    /// Never, under any circumstances, use .`len()` == 0 to check
    /// if the partition is empty, use [`ReadTransaction::is_empty`] instead.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open_transactional()?;
    /// # let partition = keyspace.open_partition("default", PartitionCreateOptions::default())?;
    /// partition.insert("a", "my_value")?;
    /// partition.insert("b", "my_value2")?;
    ///
    /// let tx = keyspace.read_tx();
    /// assert_eq!(2, tx.len(&partition)?);
    ///
    /// partition.insert("c", "my_value3")?;
    ///
    /// // Repeatable read
    /// assert_eq!(2, tx.len(&partition)?);
    ///
    /// // Start new snapshot
    /// let tx = keyspace.read_tx();
    /// assert_eq!(3, tx.len(&partition)?);
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn len(&self, partition: &TxPartitionHandle) -> crate::Result<usize> {
        let mut count = 0;

        for kv in self.iter(partition) {
            let _ = kv?;
            count += 1;
        }

        Ok(count)
    }

    /// Returns `true` if the partition is empty.
    ///
    /// This operation has O(1) complexity.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open_transactional()?;
    /// # let partition = keyspace.open_partition("default", PartitionCreateOptions::default())?;
    /// assert!(keyspace.read_tx().is_empty(&partition)?);
    ///
    /// partition.insert("a", "abc")?;
    /// assert!(!keyspace.read_tx().is_empty(&partition)?);
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn is_empty(&self, partition: &TxPartitionHandle) -> crate::Result<bool> {
        self.first_key_value(partition).map(|x| x.is_none())
    }

    /// Iterates over the transaction's state.
    ///
    /// Avoid using this function, or limit it as otherwise it may scan a lot of items.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open_transactional()?;
    /// # let partition = keyspace.open_partition("default", PartitionCreateOptions::default())?;
    /// partition.insert("a", "abc")?;
    /// partition.insert("f", "abc")?;
    /// partition.insert("g", "abc")?;
    ///
    /// assert_eq!(3, keyspace.read_tx().iter(&partition).count());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    #[must_use]
    pub fn iter<'a>(
        &'a self,
        partition: &'a TxPartitionHandle,
    ) -> impl DoubleEndedIterator<Item = crate::Result<KvPair>> + 'static {
        let iter = partition
            .inner
            .tree
            .iter(Some(self.nonce.instant), None)
            .map(|item| Ok(item?));

        crate::iter::Iter::new(self.nonce.clone(), iter)
    }

    /// Iterates over the transaction's state, returning keys only.
    ///
    /// Avoid using this function, or limit it as otherwise it may scan a lot of items.
    #[must_use]
    pub fn keys<'a>(
        &'a self,
        partition: &'a TxPartitionHandle,
    ) -> impl DoubleEndedIterator<Item = crate::Result<UserKey>> + 'static {
        let iter = partition
            .inner
            .tree
            .keys(Some(self.nonce.instant), None)
            .map(|item| Ok(item?));

        crate::iter::Iter::new(self.nonce.clone(), iter)
    }

    /// Iterates over the transaction's state, returning values only.
    ///
    /// Avoid using this function, or limit it as otherwise it may scan a lot of items.
    #[must_use]
    pub fn values<'a>(
        &'a self,
        partition: &'a TxPartitionHandle,
    ) -> impl DoubleEndedIterator<Item = crate::Result<UserValue>> + 'static {
        let iter = partition
            .inner
            .tree
            .values(Some(self.nonce.instant), None)
            .map(|item| Ok(item?));

        crate::iter::Iter::new(self.nonce.clone(), iter)
    }

    /// Iterates over a range of the transaction's state.
    ///
    /// Avoid using full or unbounded ranges as they may scan a lot of items (unless limited).
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open_transactional()?;
    /// # let partition = keyspace.open_partition("default", PartitionCreateOptions::default())?;
    /// partition.insert("a", "abc")?;
    /// partition.insert("f", "abc")?;
    /// partition.insert("g", "abc")?;
    ///
    /// assert_eq!(2, keyspace.read_tx().range(&partition, "a"..="f").count());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    #[must_use]
    pub fn range<'a, K: AsRef<[u8]> + 'a, R: RangeBounds<K> + 'a>(
        &'a self,
        partition: &'a TxPartitionHandle,
        range: R,
    ) -> impl DoubleEndedIterator<Item = crate::Result<KvPair>> + 'static {
        // TODO: 3.0.0: if we bind the iterator lifetime to ReadTx, we can remove the snapshot nonce from Iter
        // TODO: for all other ReadTx::iterators too

        let iter = partition
            .inner
            .tree
            .range(range, Some(self.nonce.instant), None)
            .map(|item| Ok(item?));

        crate::iter::Iter::new(self.nonce.clone(), iter)
    }

    /// Iterates over a prefixed set of the transaction's state.
    ///
    /// Avoid using an empty prefix as it may scan a lot of items (unless limited).
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open_transactional()?;
    /// # let partition = keyspace.open_partition("default", PartitionCreateOptions::default())?;
    /// partition.insert("a", "abc")?;
    /// partition.insert("ab", "abc")?;
    /// partition.insert("abc", "abc")?;
    ///
    /// assert_eq!(2, keyspace.read_tx().prefix(&partition, "ab").count());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    #[must_use]
    pub fn prefix<'a, K: AsRef<[u8]> + 'a>(
        &'a self,
        partition: &'a TxPartitionHandle,
        prefix: K,
    ) -> impl DoubleEndedIterator<Item = crate::Result<KvPair>> + 'static {
        let iter = partition
            .inner
            .tree
            .prefix(prefix, Some(self.nonce.instant), None)
            .map(|item| Ok(item?));

        crate::iter::Iter::new(self.nonce.clone(), iter)
    }
}
