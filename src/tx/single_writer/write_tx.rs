use crate::{
    snapshot_nonce::SnapshotNonce,
    tx::{single_writer::keyspace::SingleWriterTxKeyspace, write_tx::BaseTransaction},
    Guard, Keyspace, PersistMode, Readable, SingleWriterTxDatabase,
};
use lsm_tree::{KvPair, UserKey, UserValue};
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
    inner: BaseTransaction,
}

impl Readable for WriteTransaction<'_> {
    fn get<K: AsRef<[u8]>>(
        &self,
        keyspace: impl AsRef<Keyspace>,
        key: K,
    ) -> crate::Result<Option<UserValue>> {
        self.inner.get(keyspace, key)
    }

    fn contains_key<K: AsRef<[u8]>>(
        &self,
        keyspace: impl AsRef<Keyspace>,
        key: K,
    ) -> crate::Result<bool> {
        self.inner.contains_key(keyspace, key)
    }

    fn first_key_value(&self, keyspace: impl AsRef<Keyspace>) -> crate::Result<Option<KvPair>> {
        self.inner.first_key_value(keyspace)
    }

    fn last_key_value(&self, keyspace: impl AsRef<Keyspace>) -> crate::Result<Option<KvPair>> {
        self.inner.last_key_value(keyspace)
    }

    fn size_of<K: AsRef<[u8]>>(
        &self,
        keyspace: impl AsRef<Keyspace>,
        key: K,
    ) -> crate::Result<Option<u32>> {
        self.inner.size_of(keyspace, key)
    }

    fn iter(
        &self,
        keyspace: impl AsRef<Keyspace>,
    ) -> impl DoubleEndedIterator<Item = Guard> + 'static {
        self.inner.iter(keyspace)
    }

    fn range<K: AsRef<[u8]>, R: RangeBounds<K>>(
        &self,
        keyspace: impl AsRef<Keyspace>,
        range: R,
    ) -> impl DoubleEndedIterator<Item = Guard> + 'static {
        self.inner.range(keyspace, range)
    }

    fn prefix<K: AsRef<[u8]>>(
        &self,
        keyspace: impl AsRef<Keyspace>,
        prefix: K,
    ) -> impl DoubleEndedIterator<Item = Guard> + 'static {
        self.inner.prefix(keyspace, prefix)
    }
}

impl<'tx> WriteTransaction<'tx> {
    pub(crate) fn new(
        db: SingleWriterTxDatabase,
        nonce: SnapshotNonce,
        guard: MutexGuard<'tx, ()>,
    ) -> Self {
        Self {
            _guard: guard,
            inner: BaseTransaction::new(db.inner, nonce),
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
    /// # use fjall::{SingleWriterTxDatabase, KeyspaceCreateOptions, Readable};
    /// # use std::sync::Arc;
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = SingleWriterTxDatabase::builder(folder).open()?;
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
        keyspace: &SingleWriterTxKeyspace,
        key: K,
    ) -> crate::Result<Option<UserValue>> {
        self.inner.take(keyspace.inner(), key)
    }

    /// Atomically updates an item and returns the new value.
    ///
    /// Returning `None` removes the item if it existed before.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{SingleWriterTxDatabase, KeyspaceCreateOptions, Slice, Readable};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = SingleWriterTxDatabase::builder(folder).open()?;
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
    /// # use fjall::{SingleWriterTxDatabase, KeyspaceCreateOptions, Readable};
    /// # use std::sync::Arc;
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = SingleWriterTxDatabase::builder(folder).open()?;
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
        keyspace: &SingleWriterTxKeyspace,
        key: K,
        f: F,
    ) -> crate::Result<Option<UserValue>> {
        self.inner.update_fetch(keyspace.inner(), key, f)
    }

    /// Atomically updates an item and returns the previous value.
    ///
    /// Returning `None` removes the item if it existed before.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{SingleWriterTxDatabase, KeyspaceCreateOptions, Slice, Readable};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = SingleWriterTxDatabase::builder(folder).open()?;
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
    /// # use fjall::{SingleWriterTxDatabase, KeyspaceCreateOptions, Readable};
    /// # use std::sync::Arc;
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = SingleWriterTxDatabase::builder(folder).open()?;
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
        keyspace: &SingleWriterTxKeyspace,
        key: K,
        f: F,
    ) -> crate::Result<Option<UserValue>> {
        self.inner.fetch_update(keyspace.inner(), key, f)
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
    /// # use fjall::{SingleWriterTxDatabase, KeyspaceCreateOptions, Readable};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = SingleWriterTxDatabase::builder(folder).open()?;
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
        keyspace: &SingleWriterTxKeyspace,
        key: K,
        value: V,
    ) {
        self.inner.insert(keyspace.inner(), key, value);
    }

    /// Removes an item from the keyspace.
    ///
    /// The key may be up to 65536 bytes long.
    /// Shorter keys result in better performance.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{SingleWriterTxDatabase, KeyspaceCreateOptions, Readable};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = SingleWriterTxDatabase::builder(folder).open()?;
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
    pub fn remove<K: Into<UserKey>>(&mut self, keyspace: &SingleWriterTxKeyspace, key: K) {
        self.inner.remove(keyspace.inner(), key);
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
    /// # use fjall::{SingleWriterTxDatabase, KeyspaceCreateOptions, Readable};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = SingleWriterTxDatabase::builder(folder).open()?;
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
    pub fn remove_weak<K: Into<UserKey>>(&mut self, keyspace: &SingleWriterTxKeyspace, key: K) {
        self.inner.remove_weak(keyspace.inner(), key);
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
