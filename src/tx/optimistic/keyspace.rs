// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::Readable;
use crate::{tx::optimistic::OptimisticTxDatabase, Keyspace};
use lsm_tree::{KvPair, UserKey, UserValue};
use std::path::PathBuf;

/// Handle to a keyspace of a transactional database
#[derive(Clone)]
pub struct OptimisticTxKeyspace {
    pub(crate) inner: Keyspace,
    pub(crate) db: OptimisticTxDatabase,
}

impl AsRef<Keyspace> for OptimisticTxKeyspace {
    fn as_ref(&self) -> &Keyspace {
        self.inner()
    }
}

impl OptimisticTxKeyspace {
    /// Returns the underlying LSM-tree's path.
    #[must_use]
    pub fn path(&self) -> PathBuf {
        self.inner.path().into()
    }

    /// Approximates the amount of items in the keyspace.
    ///
    /// For update- or delete-heavy workloads, this value will
    /// diverge from the real value, but is a O(1) operation.
    ///
    /// For insert-only workloads (e.g. logs, time series)
    /// this value is reliable.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{OptimisticTxDatabase, KeyspaceCreateOptions, Readable};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = OptimisticTxDatabase::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;
    /// assert_eq!(tree.approximate_len(), 0);
    ///
    /// tree.insert("1", "abc")?;
    /// assert_eq!(tree.approximate_len(), 1);
    ///
    /// tree.remove("1")?;
    /// // Oops! approximate_len will not be reliable here
    /// assert_eq!(tree.approximate_len(), 2);
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    #[must_use]
    pub fn approximate_len(&self) -> usize {
        self.inner.approximate_len()
    }

    /// Removes an item and returns its value if it existed.
    ///
    /// The operation will run wrapped in a transaction.
    ///
    /// ```
    /// # use fjall::{OptimisticTxDatabase, KeyspaceCreateOptions, Readable};
    /// # use std::sync::Arc;
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = OptimisticTxDatabase::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;
    /// tree.insert("a", "abc")?;
    ///
    /// let taken = tree.take("a")?.unwrap();
    /// assert_eq!(b"abc", &*taken);
    ///
    /// let item = tree.get("a")?;
    /// assert!(item.is_none());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn take<K: Into<UserKey>>(&self, key: K) -> crate::Result<Option<UserValue>> {
        self.fetch_update(key, |_| None)
    }

    /// Atomically updates an item and returns the previous value.
    ///
    /// Returning `None` removes the item if it existed before.
    ///
    /// The operation will run wrapped in a transaction.
    ///
    /// # Note
    ///
    /// The provided closure can be called multiple times as this function
    /// automatically retries on conflict. Since this is an `FnMut`, make sure
    /// it is idempotent and will not cause side-effects.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{OptimisticTxDatabase, Slice, KeyspaceCreateOptions};
    /// # use std::sync::Arc;
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = OptimisticTxDatabase::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;
    /// tree.insert("a", "abc")?;
    ///
    /// let prev = tree.fetch_update("a", |_| Some(Slice::from(*b"def")))?.unwrap();
    /// assert_eq!(b"abc", &*prev);
    ///
    /// let item = tree.get("a")?;
    /// assert_eq!(Some("def".as_bytes().into()), item);
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// ```
    /// # use fjall::{OptimisticTxDatabase, KeyspaceCreateOptions, Readable};
    /// # use std::sync::Arc;
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = OptimisticTxDatabase::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;
    /// tree.insert("a", "abc")?;
    ///
    /// let prev = tree.fetch_update("a", |_| None)?.unwrap();
    /// assert_eq!(b"abc", &*prev);
    ///
    /// let item = tree.get("a")?;
    /// assert!(item.is_none());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    #[allow(unused_mut)]
    pub fn fetch_update<K: Into<UserKey>, F: FnMut(Option<&UserValue>) -> Option<UserValue>>(
        &self,
        key: K,
        mut f: F,
    ) -> crate::Result<Option<UserValue>> {
        let key = key.into();

        loop {
            let mut tx = self.db.write_tx()?;
            let prev = tx.fetch_update(self.inner(), key.clone(), &mut f)?;
            if tx.commit()?.is_ok() {
                return Ok(prev);
            }
        }
    }

    /// Atomically updates an item and returns the new value.
    ///
    /// Returning `None` removes the item if it existed before.
    ///
    /// The operation will run wrapped in a transaction.
    ///
    /// # Note
    ///
    /// The provided closure can be called multiple times as this function
    /// automatically retries on conflict. Since this is an `FnMut`, make sure
    /// it is idempotent and will not cause side-effects.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{OptimisticTxDatabase, Slice, KeyspaceCreateOptions};
    /// # use std::sync::Arc;
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = OptimisticTxDatabase::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;
    /// tree.insert("a", "abc")?;
    ///
    /// let updated = tree.update_fetch("a", |_| Some(Slice::from(*b"def")))?.unwrap();
    /// assert_eq!(b"def", &*updated);
    ///
    /// let item = tree.get("a")?;
    /// assert_eq!(Some("def".as_bytes().into()), item);
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// ```
    /// # use fjall::{OptimisticTxDatabase, KeyspaceCreateOptions, Readable};
    /// # use std::sync::Arc;
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = OptimisticTxDatabase::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;
    /// tree.insert("a", "abc")?;
    ///
    /// let updated = tree.update_fetch("a", |_| None)?;
    /// assert!(updated.is_none());
    ///
    /// let item = tree.get("a")?;
    /// assert!(item.is_none());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    #[allow(unused_mut)]
    pub fn update_fetch<K: Into<UserKey>, F: FnMut(Option<&UserValue>) -> Option<UserValue>>(
        &self,
        key: K,
        mut f: F,
    ) -> crate::Result<Option<UserValue>> {
        let key = key.into();

        loop {
            let mut tx = self.db.write_tx()?;
            let updated = tx.update_fetch(self.inner(), key.clone(), &mut f)?;
            if tx.commit()?.is_ok() {
                return Ok(updated);
            }
        }
    }

    /// Inserts a key-value pair into the keyspace.
    ///
    /// Keys may be up to 65536 bytes long, values up to 2^32 bytes.
    /// Shorter keys and values result in better performance.
    ///
    /// If the key already exists, the item will be overwritten.
    ///
    /// The operation will run wrapped in a transaction.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{OptimisticTxDatabase, KeyspaceCreateOptions, Readable};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = OptimisticTxDatabase::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;
    /// tree.insert("a", "abc")?;
    ///
    /// assert!(!db.read_tx().is_empty(&tree)?);
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn insert<K: Into<UserKey>, V: Into<UserValue>>(
        &self,
        key: K,
        value: V,
    ) -> crate::Result<()> {
        let mut tx = self.db.write_tx()?;
        tx.insert(self.inner(), key, value);

        #[allow(clippy::expect_used)]
        tx.commit()?.expect("blind insert should not conflict ever");

        Ok(())
    }

    /// Removes an item from the keyspace.
    ///
    /// The key may be up to 65536 bytes long.
    /// Shorter keys result in better performance.
    ///
    /// The operation will run wrapped in a transaction.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{OptimisticTxDatabase, KeyspaceCreateOptions, Readable};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = OptimisticTxDatabase::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;
    /// tree.insert("a", "abc")?;
    /// assert!(!db.read_tx().is_empty(&tree)?);
    ///
    /// tree.remove("a")?;
    /// assert!(db.read_tx().is_empty(&tree)?);
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn remove<K: Into<UserKey>>(&self, key: K) -> crate::Result<()> {
        let mut tx = self.db.write_tx()?;
        tx.remove(self.inner(), key);

        #[allow(clippy::expect_used)]
        tx.commit()?.expect("blind remove should not conflict ever");

        Ok(())
    }

    /// Removes an item from the keyspace, leaving behind a weak tombstone.
    ///
    /// The tombstone marker of this delete operation will vanish when it
    /// collides with its corresponding insertion.
    /// This may cause older versions of the value to be resurrected, so it should
    /// only be used and preferred in scenarios where a key is only ever written once.
    ///
    /// The key may be up to 65536 bytes long.
    /// Shorter keys result in better performance.
    ///
    /// The operation will run wrapped in a transaction.
    ///
    /// # Experimental
    ///
    /// This function is currently experimental.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{OptimisticTxDatabase, KeyspaceCreateOptions, Readable};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = OptimisticTxDatabase::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;
    /// tree.insert("a", "abc")?;
    /// assert!(!db.read_tx().is_empty(&tree)?);
    ///
    /// tree.remove_weak("a")?;
    /// assert!(db.read_tx().is_empty(&tree)?);
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    #[doc(hidden)]
    pub fn remove_weak<K: Into<UserKey>>(&self, key: K) -> crate::Result<()> {
        let mut tx = self.db.write_tx()?;
        tx.remove_weak(self.inner(), key);

        #[allow(clippy::expect_used)]
        tx.commit()?.expect("blind remove should not conflict ever");

        Ok(())
    }

    /// Retrieves an item from the keyspace.
    ///
    /// The operation will run wrapped in a read snapshot.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{OptimisticTxDatabase, KeyspaceCreateOptions, Readable};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = OptimisticTxDatabase::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;
    /// tree.insert("a", "my_value")?;
    ///
    /// let item = tree.get("a")?;
    /// assert_eq!(Some("my_value".as_bytes().into()), item);
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> crate::Result<Option<lsm_tree::UserValue>> {
        self.inner.get(key)
    }

    /// Retrieves the size of an item from the keyspace.
    ///
    /// The operation will run wrapped in a read snapshot.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{OptimisticTxDatabase, KeyspaceCreateOptions, Readable};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = OptimisticTxDatabase::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;
    /// tree.insert("a", "my_value")?;
    ///
    /// let len = tree.size_of("a")?.unwrap_or_default();
    /// assert_eq!("my_value".len() as u32, len);
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn size_of<K: AsRef<[u8]>>(&self, key: K) -> crate::Result<Option<u32>> {
        self.inner.size_of(key)
    }

    /// Returns the first key-value pair in the keyspace.
    /// The key in this pair is the minimum key in the keyspace.
    ///
    /// The operation will run wrapped in a read snapshot.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{OptimisticTxDatabase, KeyspaceCreateOptions, Readable};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = OptimisticTxDatabase::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;
    /// tree.insert("a", "my_value")?;
    /// tree.insert("b", "my_value")?;
    ///
    /// assert_eq!(b"a", &*tree.first_key_value()?.unwrap().0);
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn first_key_value(&self) -> crate::Result<Option<KvPair>> {
        let read_tx = self.db.read_tx();
        read_tx.first_key_value(self)
    }

    /// Returns the last key-value pair in the keyspace.
    /// The key in this pair is the maximum key in the keyspace.
    ///
    /// The operation will run wrapped in a read snapshot.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{OptimisticTxDatabase, KeyspaceCreateOptions, Readable};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = OptimisticTxDatabase::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;
    /// tree.insert("a", "my_value")?;
    /// tree.insert("b", "my_value")?;
    ///
    /// assert_eq!(b"b", &*tree.last_key_value()?.unwrap().0);
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn last_key_value(&self) -> crate::Result<Option<KvPair>> {
        let read_tx = self.db.read_tx();
        read_tx.last_key_value(self)
    }

    /// Returns `true` if the keyspace contains the specified key.
    ///
    /// The operation will run wrapped in a read snapshot.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{OptimisticTxDatabase, KeyspaceCreateOptions, Readable};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = OptimisticTxDatabase::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;
    /// tree.insert("a", "my_value")?;
    ///
    /// assert!(tree.contains_key("a")?);
    /// assert!(!tree.contains_key("b")?);
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn contains_key<K: AsRef<[u8]>>(&self, key: K) -> crate::Result<bool> {
        self.inner.contains_key(key)
    }

    /// Allows access to the inner keyspace handle, allowing to
    /// escape from the transactional context.
    #[doc(hidden)]
    #[must_use]
    pub fn inner(&self) -> &Keyspace {
        &self.inner
    }
}
