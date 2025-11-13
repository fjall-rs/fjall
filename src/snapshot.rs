// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{snapshot_nonce::SnapshotNonce, Guard, Keyspace};
use lsm_tree::{AbstractTree, KvPair, UserValue};
use std::ops::RangeBounds;

/// A cross-keyspace snapshot
///
/// Snapshots keep a consistent view of the database at the time,
/// meaning old data will not be dropped until it is not referenced by any active transaction.
///
/// For that reason, you should try to keep transactions short-lived, and make sure they
/// are not held somewhere *forever*.
#[clippy::has_significant_drop]
pub struct Snapshot {
    pub(crate) nonce: SnapshotNonce,
}

impl Snapshot {
    pub(crate) fn new(nonce: SnapshotNonce) -> Self {
        Self { nonce }
    }

    /// Retrieves an item from the transaction's state.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Database, KeyspaceCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = Database::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;
    /// tree.insert("a", "my_value")?;
    ///
    /// let snapshot = db.snapshot();
    /// let item = snapshot.get(&tree, "a")?;
    /// assert_eq!(Some("my_value".as_bytes().into()), item);
    ///
    /// tree.insert("b", "my_updated_value")?;
    ///
    /// // Repeatable read
    /// let item = snapshot.get(&tree, "a")?;
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
        keyspace: &Keyspace,
        key: K,
    ) -> crate::Result<Option<UserValue>> {
        keyspace
            .tree
            .get(key, self.nonce.instant)
            .map_err(Into::into)
    }

    /// Retrieves the size of an item from the transaction's state.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Database, KeyspaceCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = Database::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;
    /// tree.insert("a", "my_value")?;
    ///
    /// let snapshot = db.snapshot();
    /// let item = snapshot.size_of(&tree, "a")?.unwrap_or_default();
    /// assert_eq!("my_value".len() as u32, item);
    ///
    /// tree.insert("b", "my_updated_value")?;
    ///
    /// // Repeatable read
    /// let item = snapshot.size_of(&tree, "a")?.unwrap_or_default();
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
        keyspace: &Keyspace,
        key: K,
    ) -> crate::Result<Option<u32>> {
        keyspace
            .tree
            .size_of(key, self.nonce.instant)
            .map_err(Into::into)
    }

    /// Returns `true` if the transaction's state contains the specified key.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Database, KeyspaceCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = Database::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;
    /// tree.insert("a", "my_value")?;
    ///
    /// let snapshot = db.snapshot();
    /// assert!(snapshot.contains_key(&tree, "a")?);
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn contains_key<K: AsRef<[u8]>>(&self, keyspace: &Keyspace, key: K) -> crate::Result<bool> {
        keyspace
            .tree
            .contains_key(key, self.nonce.instant)
            .map_err(Into::into)
    }

    /// Returns the first key-value pair in the transaction's state.
    /// The key in this pair is the minimum key in the transaction's state.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Database, KeyspaceCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = Database::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;
    /// tree.insert("1", "abc")?;
    /// tree.insert("3", "abc")?;
    /// tree.insert("5", "abc")?;
    ///
    /// let (key, _) = db.snapshot().first_key_value(&tree)?.expect("item should exist");
    /// assert_eq!(&*key, "1".as_bytes());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn first_key_value(&self, keyspace: &Keyspace) -> crate::Result<Option<KvPair>> {
        self.iter(keyspace)
            .next()
            .map(Guard::into_inner)
            .transpose()
    }

    /// Returns the last key-value pair in the transaction's state.
    /// The key in this pair is the maximum key in the transaction's state.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Database, KeyspaceCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = Database::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;
    /// tree.insert("1", "abc")?;
    /// tree.insert("3", "abc")?;
    /// tree.insert("5", "abc")?;
    ///
    /// let (key, _) = db.snapshot().last_key_value(&tree)?.expect("item should exist");
    /// assert_eq!(&*key, "5".as_bytes());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn last_key_value(&self, keyspace: &Keyspace) -> crate::Result<Option<KvPair>> {
        self.iter(keyspace)
            .next_back()
            .map(Guard::into_inner)
            .transpose()
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
    /// # use fjall::{Database, KeyspaceCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = Database::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;
    /// tree.insert("a", "my_value")?;
    /// tree.insert("b", "my_value2")?;
    ///
    /// let snapshot = db.snapshot();
    /// assert_eq!(2, snapshot.len(&tree)?);
    ///
    /// tree.insert("c", "my_value3")?;
    ///
    /// // Repeatable read
    /// assert_eq!(2, snapshot.len(&tree)?);
    ///
    /// // Start new snapshot
    /// let snapshot = db.snapshot();
    /// assert_eq!(3, snapshot.len(&tree)?);
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn len(&self, keyspace: &Keyspace) -> crate::Result<usize> {
        let mut count = 0;

        for guard in self.iter(keyspace) {
            let _ = guard.key()?;
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
    /// # use fjall::{Database, KeyspaceCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = Database::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;
    /// assert!(db.snapshot().is_empty(&tree)?);
    ///
    /// tree.insert("a", "abc")?;
    /// assert!(!db.snapshot().is_empty(&tree)?);
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn is_empty(&self, keyspace: &Keyspace) -> crate::Result<bool> {
        self.first_key_value(keyspace).map(|x| x.is_none())
    }

    /// Iterates over the transaction's state.
    ///
    /// Avoid using this function, or limit it as otherwise it may scan a lot of items.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Database, KeyspaceCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = Database::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;
    /// tree.insert("a", "abc")?;
    /// tree.insert("f", "abc")?;
    /// tree.insert("g", "abc")?;
    ///
    /// assert_eq!(3, db.snapshot().iter(&tree).count());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    #[must_use]
    pub fn iter(&'_ self, keyspace: &'_ Keyspace) -> impl DoubleEndedIterator<Item = Guard> + '_ {
        keyspace.tree.iter(self.nonce.instant, None).map(Guard)
    }

    /// Iterates over a range of the transaction's state.
    ///
    /// Avoid using full or unbounded ranges as they may scan a lot of items (unless limited).
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Database, KeyspaceCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = Database::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;
    /// tree.insert("a", "abc")?;
    /// tree.insert("f", "abc")?;
    /// tree.insert("g", "abc")?;
    ///
    /// assert_eq!(2, db.snapshot().range(&tree, "a"..="f").count());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    #[must_use]
    pub fn range<'a, K: AsRef<[u8]> + 'a, R: RangeBounds<K> + 'a>(
        &'a self,
        keyspace: &'a Keyspace,
        range: R,
    ) -> impl DoubleEndedIterator<Item = Guard> + 'a {
        keyspace
            .tree
            .range(range, self.nonce.instant, None)
            .map(Guard)
    }

    /// Iterates over a prefixed set of the transaction's state.
    ///
    /// Avoid using an empty prefix as it may scan a lot of items (unless limited).
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Database, KeyspaceCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = Database::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;
    /// tree.insert("a", "abc")?;
    /// tree.insert("ab", "abc")?;
    /// tree.insert("abc", "abc")?;
    ///
    /// assert_eq!(2, db.snapshot().prefix(&tree, "ab").count());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    #[must_use]
    pub fn prefix<'a, K: AsRef<[u8]> + 'a>(
        &'a self,
        keyspace: &'a Keyspace,
        prefix: K,
    ) -> impl DoubleEndedIterator<Item = Guard> + 'a {
        keyspace
            .tree
            .prefix(prefix, self.nonce.instant, None)
            .map(Guard)

        // crate::iter::Iter::new(self.nonce.clone(), iter)
    }
}
