// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{snapshot_nonce::SnapshotNonce, TxKeyspace};
use lsm_tree::{AbstractTree, KvPair, UserKey, UserValue};
use std::ops::RangeBounds;

/// A cross-keyspace, read-only transaction (snapshot)
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
    /// # use fjall::{TxDatabase, KeyspaceCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = TxDatabase::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;
    /// tree.insert("a", "my_value")?;
    ///
    /// let tx = db.read_tx();
    /// let item = tx.get(&tree, "a")?;
    /// assert_eq!(Some("my_value".as_bytes().into()), item);
    ///
    /// tree.insert("b", "my_updated_value")?;
    ///
    /// // Repeatable read
    /// let item = tx.get(&tree, "a")?;
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
        keyspace: &TxKeyspace,
        key: K,
    ) -> crate::Result<Option<UserValue>> {
        keyspace
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
    /// # use fjall::{TxDatabase, KeyspaceCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = TxDatabase::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;
    /// tree.insert("a", "my_value")?;
    ///
    /// let tx = db.read_tx();
    /// let item = tx.size_of(&tree, "a")?.unwrap_or_default();
    /// assert_eq!("my_value".len() as u32, item);
    ///
    /// tree.insert("b", "my_updated_value")?;
    ///
    /// // Repeatable read
    /// let item = tx.size_of(&tree, "a")?.unwrap_or_default();
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
        keyspace: &TxKeyspace,
        key: K,
    ) -> crate::Result<Option<u32>> {
        keyspace
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
    /// # use fjall::{TxDatabase, KeyspaceCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = TxDatabase::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;
    /// tree.insert("a", "my_value")?;
    ///
    /// let tx = db.read_tx();
    /// assert!(tx.contains_key(&tree, "a")?);
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn contains_key<K: AsRef<[u8]>>(
        &self,
        keyspace: &TxKeyspace,
        key: K,
    ) -> crate::Result<bool> {
        keyspace
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
    /// # use fjall::{TxDatabase, KeyspaceCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = TxDatabase::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;
    /// tree.insert("1", "abc")?;
    /// tree.insert("3", "abc")?;
    /// tree.insert("5", "abc")?;
    ///
    /// let (key, _) = db.read_tx().first_key_value(&tree)?.expect("item should exist");
    /// assert_eq!(&*key, "1".as_bytes());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn first_key_value(&self, keyspace: &TxKeyspace) -> crate::Result<Option<KvPair>> {
        self.iter(keyspace).next().transpose()
    }

    /// Returns the last key-value pair in the transaction's state.
    /// The key in this pair is the maximum key in the transaction's state.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{TxDatabase, KeyspaceCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = TxDatabase::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;
    /// tree.insert("1", "abc")?;
    /// tree.insert("3", "abc")?;
    /// tree.insert("5", "abc")?;
    ///
    /// let (key, _) = db.read_tx().last_key_value(&tree)?.expect("item should exist");
    /// assert_eq!(&*key, "5".as_bytes());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn last_key_value(&self, keyspace: &TxKeyspace) -> crate::Result<Option<KvPair>> {
        self.iter(keyspace).next_back().transpose()
    }

    /// Scans the entire keyspace, returning the amount of items.
    ///
    /// ###### Caution
    ///
    /// This operation scans the entire keyspace: O(n) complexity!
    ///
    /// Never, under any circumstances, use .`len()` == 0 to check
    /// if the keyspace is empty, use [`ReadTransaction::is_empty`] instead.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{TxDatabase, KeyspaceCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = TxDatabase::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;
    /// tree.insert("a", "my_value")?;
    /// tree.insert("b", "my_value2")?;
    ///
    /// let tx = db.read_tx();
    /// assert_eq!(2, tx.len(&tree)?);
    ///
    /// tree.insert("c", "my_value3")?;
    ///
    /// // Repeatable read
    /// assert_eq!(2, tx.len(&tree)?);
    ///
    /// // Start new snapshot
    /// let tx = db.read_tx();
    /// assert_eq!(3, tx.len(&tree)?);
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn len(&self, keyspace: &TxKeyspace) -> crate::Result<usize> {
        let mut count = 0;

        for kv in self.iter(keyspace) {
            let _ = kv?;
            count += 1;
        }

        Ok(count)
    }

    /// Returns `true` if the parkeyspacetition is empty.
    ///
    /// This operation has O(log N) complexity.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{TxDatabase, KeyspaceCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = TxDatabase::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;
    /// assert!(db.read_tx().is_empty(&tree)?);
    ///
    /// tree.insert("a", "abc")?;
    /// assert!(!db.read_tx().is_empty(&tree)?);
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn is_empty(&self, keyspace: &TxKeyspace) -> crate::Result<bool> {
        self.first_key_value(keyspace).map(|x| x.is_none())
    }

    /// Iterates over the transaction's state.
    ///
    /// Avoid using this function, or limit it as otherwise it may scan a lot of items.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{TxDatabase, KeyspaceCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = TxDatabase::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;
    /// tree.insert("a", "abc")?;
    /// tree.insert("f", "abc")?;
    /// tree.insert("g", "abc")?;
    ///
    /// assert_eq!(3, db.read_tx().iter(&tree).count());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    #[must_use]
    pub fn iter<'a>(
        &'a self,
        keyspace: &'a TxKeyspace,
    ) -> impl DoubleEndedIterator<Item = crate::Result<KvPair>> + 'static {
        let iter = keyspace
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
        keyspace: &'a TxKeyspace,
    ) -> impl DoubleEndedIterator<Item = crate::Result<UserKey>> + 'static {
        let iter = keyspace
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
        keyspace: &'a TxKeyspace,
    ) -> impl DoubleEndedIterator<Item = crate::Result<UserValue>> + 'static {
        let iter = keyspace
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
    /// # use fjall::{TxDatabase, KeyspaceCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = TxDatabase::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;
    /// tree.insert("a", "abc")?;
    /// tree.insert("f", "abc")?;
    /// tree.insert("g", "abc")?;
    ///
    /// assert_eq!(2, db.read_tx().range(&tree, "a"..="f").count());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    #[must_use]
    pub fn range<'a, K: AsRef<[u8]> + 'a, R: RangeBounds<K> + 'a>(
        &'a self,
        keyspace: &'a TxKeyspace,
        range: R,
    ) -> impl DoubleEndedIterator<Item = crate::Result<KvPair>> + 'static {
        // TODO: 3.0.0: if we bind the iterator lifetime to ReadTx, we can remove the snapshot nonce from Iter
        // TODO: for all other ReadTx::iterators too

        let iter = keyspace
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
    /// # use fjall::{TxDatabase, KeyspaceCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = TxDatabase::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;
    /// tree.insert("a", "abc")?;
    /// tree.insert("ab", "abc")?;
    /// tree.insert("abc", "abc")?;
    ///
    /// assert_eq!(2, db.read_tx().prefix(&tree, "ab").count());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    #[must_use]
    pub fn prefix<'a, K: AsRef<[u8]> + 'a>(
        &'a self,
        keyspace: &'a TxKeyspace,
        prefix: K,
    ) -> impl DoubleEndedIterator<Item = crate::Result<KvPair>> + 'static {
        let iter = keyspace
            .inner
            .tree
            .prefix(prefix, Some(self.nonce.instant), None)
            .map(|item| Ok(item?));

        crate::iter::Iter::new(self.nonce.clone(), iter)
    }
}
