use crate::{Instant, TxPartitionHandle};
use lsm_tree::{UserKey, UserValue};
use std::ops::RangeBounds;

/// A cross-partition, read-only transaction (snapshot)
pub struct ReadTransaction {
    instant: Instant,
}

impl ReadTransaction {
    pub(crate) fn new(instant: Instant) -> Self {
        Self { instant }
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
        Ok(partition.inner.snapshot_at(self.instant).get(key)?)
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
        self.get(partition, key).map(|x| x.is_some())
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
    pub fn first_key_value(
        &self,
        partition: &TxPartitionHandle,
    ) -> crate::Result<Option<(UserKey, UserValue)>> {
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
    pub fn last_key_value(
        &self,
        partition: &TxPartitionHandle,
    ) -> crate::Result<Option<(UserKey, UserValue)>> {
        self.iter(partition).next_back().transpose()
    }

    /// Scans the entire partition, returning the amount of items.
    ///
    /// ###### Caution
    ///
    /// This operation scans the entire partition: O(n) complexity!
    ///
    /// Never, under any circumstances, use .`len()` == 0 to check
    /// if the partition is empty, use [`PartitionHandle::is_empty`] instead.
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
    ) -> impl DoubleEndedIterator<Item = crate::Result<(UserKey, UserValue)>> + 'static {
        partition
            .inner
            .tree
            .create_iter(Some(self.instant), None)
            .map(|item| Ok(item?))
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
    ) -> impl DoubleEndedIterator<Item = crate::Result<(UserKey, UserValue)>> + 'static {
        partition
            .inner
            .tree
            .create_range(&range, Some(self.instant), None)
            .map(|item| Ok(item?))
    }

    /// Iterates over a range of the transaction's state.
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
    ) -> impl DoubleEndedIterator<Item = crate::Result<(UserKey, UserValue)>> + 'static {
        partition
            .inner
            .tree
            .create_prefix(prefix, Some(self.instant), None)
            .map(|item| Ok(item?))
    }
}
