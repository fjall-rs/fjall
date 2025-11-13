use super::BaseTransaction as InnerWriteTransaction;
use crate::{snapshot_nonce::SnapshotNonce, PersistMode, TxDatabase, TxKeyspace};
use lsm_tree::{Guard, KvPair, UserKey, UserValue};
use std::{ops::RangeBounds, sync::MutexGuard};

/// A single-writer (serialized) cross-keyspace transaction
///
/// Use [`WriteTransaction::commit`] to commit changes to the keyspace(s).
///
/// Transactions keep a consistent view of the database at the time,
/// meaning old data will not be dropped until it is not referenced by any active transaction.
///
/// For that reason, you should try to keep transactions short-lived, and make sure they
/// are not held somewhere forever.
#[clippy::has_significant_drop]
pub struct WriteTransaction<'a> {
    _guard: MutexGuard<'a, ()>,
    inner: InnerWriteTransaction,
}

impl<'a> WriteTransaction<'a> {
    pub(crate) fn new(db: TxDatabase, nonce: SnapshotNonce, guard: MutexGuard<'a, ()>) -> Self {
        Self {
            _guard: guard,
            inner: InnerWriteTransaction::new(db, nonce),
        }
    }

    /// Sets the durability level.
    #[must_use]
    pub fn durability(mut self, mode: Option<PersistMode>) -> Self {
        self.inner = self.inner.durability(mode);
        self
    }

    /// Removes an item and returns its value if it existed.
    ///
    /// ```
    /// # use fjall::{TxDatabase, KeyspaceCreateOptions};
    /// # use std::sync::Arc;
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = TxDatabase::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;
    /// tree.insert("a", "abc")?;
    ///
    /// let mut tx = db.write_tx();
    ///
    /// let taken = tx.take(&tree, "a")?.unwrap();
    /// assert_eq!(b"abc", &*taken);
    /// tx.commit()?;
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
    pub fn take<K: Into<UserKey>>(
        &mut self,
        keyspace: &TxKeyspace,
        key: K,
    ) -> crate::Result<Option<UserValue>> {
        self.inner.take(keyspace, key)
    }

    /// Atomically updates an item and returns the new value.
    ///
    /// Returning `None` removes the item if it existed before.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{TxDatabase, KeyspaceCreateOptions, Slice};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = TxDatabase::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;
    /// tree.insert("a", "abc")?;
    ///
    /// let mut tx = db.write_tx();
    ///
    /// let updated = tx.update_fetch(&tree, "a", |_| Some(Slice::from(*b"def")))?.unwrap();
    /// assert_eq!(b"def", &*updated);
    /// tx.commit()?;
    ///
    /// let item = tree.get("a")?;
    /// assert_eq!(Some("def".as_bytes().into()), item);
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// ```
    /// # use fjall::{TxDatabase, KeyspaceCreateOptions};
    /// # use std::sync::Arc;
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = TxDatabase::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;
    /// tree.insert("a", "abc")?;
    ///
    /// let mut tx = db.write_tx();
    ///
    /// let updated = tx.update_fetch(&tree, "a", |_| None)?;
    /// assert!(updated.is_none());
    /// tx.commit()?;
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
    pub fn update_fetch<K: Into<UserKey>, F: FnMut(Option<&UserValue>) -> Option<UserValue>>(
        &mut self,
        keyspace: &TxKeyspace,
        key: K,
        f: F,
    ) -> crate::Result<Option<UserValue>> {
        self.inner.update_fetch(keyspace, key, f)
    }

    /// Atomically updates an item and returns the previous value.
    ///
    /// Returning `None` removes the item if it existed before.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{TxDatabase, KeyspaceCreateOptions, Slice};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = TxDatabase::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;
    /// tree.insert("a", "abc")?;
    ///
    /// let mut tx = db.write_tx();
    ///
    /// let prev = tx.fetch_update(&tree, "a", |_| Some(Slice::from(*b"def")))?.unwrap();
    /// assert_eq!(b"abc", &*prev);
    /// tx.commit()?;
    ///
    /// let item = tree.get("a")?;
    /// assert_eq!(Some("def".as_bytes().into()), item);
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// ```
    /// # use fjall::{TxDatabase, KeyspaceCreateOptions};
    /// # use std::sync::Arc;
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = TxDatabase::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;
    /// tree.insert("a", "abc")?;
    ///
    /// let mut tx = db.write_tx();
    ///
    /// let prev = tx.fetch_update(&tree, "a", |_| None)?.unwrap();
    /// assert_eq!(b"abc", &*prev);
    /// tx.commit()?;
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
    pub fn fetch_update<K: Into<UserKey>, F: FnMut(Option<&UserValue>) -> Option<UserValue>>(
        &mut self,
        keyspace: &TxKeyspace,
        key: K,
        f: F,
    ) -> crate::Result<Option<UserValue>> {
        self.inner.fetch_update(keyspace, key, f)
    }

    /// Retrieves an item from the transaction's state.
    ///
    /// The transaction allows reading your own writes (RYOW).
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{TxDatabase, KeyspaceCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = TxDatabase::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;
    /// tree.insert("a", "previous_value")?;
    /// assert_eq!(b"previous_value", &*tree.get("a")?.unwrap());
    ///
    /// let mut tx = db.write_tx();
    /// tx.insert(&tree, "a", "new_value");
    ///
    /// // Read-your-own-write
    /// let item = tx.get(&tree, "a")?;
    /// assert_eq!(Some("new_value".as_bytes().into()), item);
    ///
    /// drop(tx);
    ///
    /// // Write was not committed
    /// assert_eq!(b"previous_value", &*tree.get("a")?.unwrap());
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
        self.inner.get(keyspace, key)
    }

    /// Retrieves an item from the transaction's state.
    ///
    /// The transaction allows reading your own writes (RYOW).
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{TxDatabase, KeyspaceCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = TxDatabase::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;
    /// tree.insert("a", "previous_value")?;
    /// assert_eq!(b"previous_value", &*tree.get("a")?.unwrap());
    ///
    /// let mut tx = db.write_tx();
    /// tx.insert(&tree, "a", "new_value");
    ///
    /// // Read-your-own-write
    /// let len = tx.size_of(&tree, "a")?.unwrap_or_default();
    /// assert_eq!("new_value".len() as u32, len);
    ///
    /// drop(tx);
    ///
    /// // Write was not committed
    /// assert_eq!(b"previous_value", &*tree.get("a")?.unwrap());
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
        self.inner.size_of(keyspace, key)
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
    /// assert!(db.read_tx().contains_key(&tree, "a")?);
    ///
    /// let mut tx = db.write_tx();
    /// assert!(tx.contains_key(&tree, "a")?);
    ///
    /// tx.insert(&tree, "b", "my_value2");
    /// assert!(tx.contains_key(&tree, "b")?);
    ///
    /// // Transaction not committed yet
    /// assert!(!db.read_tx().contains_key(&tree, "b")?);
    ///
    /// tx.commit()?;
    /// assert!(db.read_tx().contains_key(&tree, "b")?);
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
        self.inner.contains_key(keyspace, key)
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
    /// #
    /// let mut tx = db.write_tx();
    /// tx.insert(&tree, "1", "abc");
    /// tx.insert(&tree, "3", "abc");
    /// tx.insert(&tree, "5", "abc");
    ///
    /// let (key, _) = tx.first_key_value(&tree)?.expect("item should exist");
    /// assert_eq!(&*key, "1".as_bytes());
    ///
    /// assert!(db.read_tx().first_key_value(&tree)?.is_none());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn first_key_value(&self, keyspace: &TxKeyspace) -> crate::Result<Option<KvPair>> {
        self.inner.first_key_value(keyspace)
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
    /// #
    /// let mut tx = db.write_tx();
    /// tx.insert(&tree, "1", "abc");
    /// tx.insert(&tree, "3", "abc");
    /// tx.insert(&tree, "5", "abc");
    ///
    /// let (key, _) = tx.last_key_value(&tree)?.expect("item should exist");
    /// assert_eq!(&*key, "5".as_bytes());
    ///
    /// assert!(db.read_tx().last_key_value(&tree)?.is_none());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn last_key_value(&self, keyspace: &TxKeyspace) -> crate::Result<Option<KvPair>> {
        self.inner.last_key_value(keyspace)
    }

    /// Scans the entire keyspace, returning the amount of items.
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
    /// let mut tx = db.write_tx();
    /// assert_eq!(2, tx.len(&tree)?);
    ///
    /// tx.insert(&tree, "c", "my_value3");
    ///
    /// // read-your-own write
    /// assert_eq!(3, tx.len(&tree)?);
    ///
    /// // Transaction is not committed yet
    /// assert_eq!(2, db.read_tx().len(&tree)?);
    ///
    /// tx.commit()?;
    /// assert_eq!(3, db.read_tx().len(&tree)?);
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn len(&self, keyspace: &TxKeyspace) -> crate::Result<usize> {
        self.inner.len(keyspace)
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
    /// #
    /// let mut tx = db.write_tx();
    /// tx.insert(&tree, "a", "abc");
    /// tx.insert(&tree, "f", "abc");
    /// tx.insert(&tree, "g", "abc");
    ///
    /// assert_eq!(3, tx.iter(&tree).count());
    /// assert_eq!(0, db.read_tx().iter(&tree).count());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    #[must_use]
    pub fn iter<'b>(
        &'b self,
        keyspace: &'b TxKeyspace,
    ) -> impl DoubleEndedIterator<Item = impl Guard> + 'b {
        self.inner.iter(keyspace)
    }

    // Iterates over a range of the transaction's state.
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
    /// #
    /// let mut tx = db.write_tx();
    /// tx.insert(&tree, "a", "abc");
    /// tx.insert(&tree, "f", "abc");
    /// tx.insert(&tree, "g", "abc");
    ///
    /// assert_eq!(2, tx.range(&tree, "a"..="f").count());
    /// assert_eq!(0, db.read_tx().range(&tree, "a"..="f").count());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    #[must_use]
    pub fn range<'b, K: AsRef<[u8]> + 'b, R: RangeBounds<K> + 'b>(
        &'b self,
        keyspace: &'b TxKeyspace,
        range: R,
    ) -> impl DoubleEndedIterator<Item = impl Guard> + 'b {
        self.inner.range(keyspace, range)
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
    /// #
    /// let mut tx = db.write_tx();
    /// tx.insert(&tree, "a", "abc");
    /// tx.insert(&tree, "ab", "abc");
    /// tx.insert(&tree, "abc", "abc");
    ///
    /// assert_eq!(2, tx.prefix(&tree, "ab").count());
    /// assert_eq!(0, db.read_tx().prefix(&tree, "ab").count());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    #[must_use]
    pub fn prefix<'b, K: AsRef<[u8]> + 'b>(
        &'b self,
        keyspace: &'b TxKeyspace,
        prefix: K,
    ) -> impl DoubleEndedIterator<Item = impl Guard> + 'b {
        self.inner.prefix(keyspace, prefix)
    }

    // Inserts a key-value pair into the keyspace.
    ///
    /// Keys may be up to 65536 bytes long, values up to 2^32 bytes.
    /// Shorter keys and values result in better performance.
    ///
    /// If the key already exists, the item will be overwritten.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{TxDatabase, KeyspaceCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = TxDatabase::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;
    /// tree.insert("a", "previous_value")?;
    /// assert_eq!(b"previous_value", &*tree.get("a")?.unwrap());
    ///
    /// let mut tx = db.write_tx();
    /// tx.insert(&tree, "a", "new_value");
    ///
    /// drop(tx);
    ///
    /// // Write was not committed
    /// assert_eq!(b"previous_value", &*tree.get("a")?.unwrap());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn insert<K: Into<UserKey>, V: Into<UserValue>>(
        &mut self,
        keyspace: &TxKeyspace,
        key: K,
        value: V,
    ) {
        self.inner.insert(keyspace, key, value);
    }

    /// Removes an item from the keyspace.
    ///
    /// The key may be up to 65536 bytes long.
    /// Shorter keys result in better performance.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{TxDatabase, KeyspaceCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = TxDatabase::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;
    /// tree.insert("a", "previous_value")?;
    /// assert_eq!(b"previous_value", &*tree.get("a")?.unwrap());
    ///
    /// let mut tx = db.write_tx();
    /// tx.remove(&tree, "a");
    ///
    /// // Read-your-own-write
    /// let item = tx.get(&tree, "a")?;
    /// assert_eq!(None, item);
    ///
    /// drop(tx);
    ///
    /// // Deletion was not committed
    /// assert_eq!(b"previous_value", &*tree.get("a")?.unwrap());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn remove<K: Into<UserKey>>(&mut self, keyspace: &TxKeyspace, key: K) {
        self.inner.remove(keyspace, key);
    }

    /// Removes an item from the keyspace, leaving behind a weak tombstone.
    ///
    /// The tombstone marker of this delete operation will vanish when it
    /// collides with its corresponding insertion.
    /// This may cause older versions of the value to be resurrected, so it should
    /// only be used and preferred in scenarios where a key is only ever written once.
    ///
    /// # Experimental
    ///
    /// This function is currently experimental.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{TxDatabase, KeyspaceCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = TxDatabase::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default())?;
    /// tree.insert("a", "previous_value")?;
    /// assert_eq!(b"previous_value", &*tree.get("a")?.unwrap());
    ///
    /// let mut tx = db.write_tx();
    /// tx.remove_weak(&tree, "a");
    ///
    /// // Read-your-own-write
    /// let item = tx.get(&tree, "a")?;
    /// assert_eq!(None, item);
    ///
    /// drop(tx);
    ///
    /// // Deletion was not committed
    /// assert_eq!(b"previous_value", &*tree.get("a")?.unwrap());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    #[doc(hidden)]
    pub fn remove_weak<K: Into<UserKey>>(&mut self, keyspace: &TxKeyspace, key: K) {
        self.inner.remove_weak(keyspace, key);
    }

    /// Commits the transaction.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn commit(self) -> crate::Result<()> {
        self.inner.commit()
    }

    /// More explicit alternative to dropping the transaction
    /// to roll it back.
    pub fn rollback(self) {
        self.inner.rollback();
    }
}
