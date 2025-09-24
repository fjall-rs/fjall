use super::BaseTransaction;
use crate::{
    snapshot_nonce::SnapshotNonce,
    tx::{conflict_manager::ConflictManager, oracle::CommitOutcome},
    PersistMode, TxDatabase, TxKeyspace,
};
use lsm_tree::{Guard, KvPair, Slice, UserKey, UserValue};
use std::{
    fmt,
    ops::{Bound, RangeBounds, RangeFull},
};

/// Transaction conflict
///
/// SSI transactions can conflict which require them to be rerun.
#[derive(Debug)]
pub struct Conflict;

impl std::error::Error for Conflict {}

impl fmt::Display for Conflict {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        "Transaction conflict".fmt(f)
    }
}

/// A SSI (Serializable Snapshot Isolation) cross-keyspace transaction
///
/// Use [`WriteTransaction::commit`] to commit changes to the keyspace(s).
///
/// Drop the transaction to rollback changes.
pub struct WriteTransaction {
    inner: BaseTransaction,
    cm: ConflictManager,
}

impl WriteTransaction {
    pub(crate) fn new(db: TxDatabase, nonce: SnapshotNonce) -> Self {
        Self {
            inner: BaseTransaction::new(db, nonce),
            cm: ConflictManager::default(),
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
    /// let mut tx = db.write_tx()?;
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
        self.fetch_update(keyspace, key, |_| None)
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
    /// let mut tx = db.write_tx()?;
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
    /// let mut tx = db.write_tx()?;
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
        let key: UserKey = key.into();

        let updated = self.inner.update_fetch(keyspace, key.clone(), f)?;

        self.cm.mark_read(keyspace.inner.id, key.clone());
        self.cm.mark_conflict(keyspace.inner.id, key);

        Ok(updated)
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
    /// let mut tx = db.write_tx()?;
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
    /// let mut tx = db.write_tx()?;
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
        let key = key.into();

        let prev = self.inner.fetch_update(keyspace, key.clone(), f)?;

        self.cm.mark_read(keyspace.inner.id, key.clone());
        self.cm.mark_conflict(keyspace.inner.id, key);

        Ok(prev)
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
    /// let mut tx = db.write_tx()?;
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
        &mut self,
        keyspace: &TxKeyspace,
        key: K,
    ) -> crate::Result<Option<UserValue>> {
        let res = self.inner.get(keyspace, key.as_ref())?;

        self.cm.mark_read(keyspace.inner.id, key.as_ref().into());

        Ok(res)
    }

    /// Retrieves the size of an item from the transaction's state.
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
    /// let mut tx = db.write_tx()?;
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
    /// Will return `Ersr` if an IO error occurs.
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
    /// let mut tx = db.write_tx()?;
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
        &mut self,
        keyspace: &TxKeyspace,
        key: K,
    ) -> crate::Result<bool> {
        let contains = self.inner.contains_key(keyspace, key.as_ref())?;

        self.cm.mark_read(keyspace.inner.id, key.as_ref().into());

        Ok(contains)
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
    /// let mut tx = db.write_tx()?;
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
    pub fn first_key_value(&mut self, keyspace: &TxKeyspace) -> crate::Result<Option<KvPair>> {
        self.iter(keyspace)
            .map(|g| g.into_inner())
            .next()
            .transpose()
            .map_err(Into::into)
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
    /// let mut tx = db.write_tx()?;
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
    pub fn last_key_value(&mut self, keyspace: &TxKeyspace) -> crate::Result<Option<KvPair>> {
        self.iter(keyspace)
            .map(|g| g.into_inner())
            .next_back()
            .transpose()
            .map_err(Into::into)
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
    /// let mut tx = db.write_tx()?;
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
    pub fn len(&mut self, keyspace: &TxKeyspace) -> crate::Result<usize> {
        let mut count = 0;

        let iter = self.iter(keyspace);

        for g in iter {
            let _ = g.key()?;
            count += 1;
        }

        Ok(count)
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
    /// let mut tx = db.write_tx()?;
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
    pub fn iter<'a>(
        &'a mut self,
        keyspace: &'a TxKeyspace,
    ) -> impl DoubleEndedIterator<Item = impl Guard + use<'a>> + 'a {
        self.cm.mark_range(keyspace.inner.id, RangeFull);

        self.inner.iter(keyspace)
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
    /// #
    /// let mut tx = db.write_tx()?;
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
        &'b mut self,
        keyspace: &'b TxKeyspace,
        range: R,
    ) -> impl DoubleEndedIterator<Item = impl Guard + use<'b, K, R>> + 'b {
        // TODO: Bound::map 1.77
        let start: Bound<Slice> = match range.start_bound() {
            Bound::Included(k) => Bound::Included(k.as_ref().into()),
            Bound::Excluded(k) => Bound::Excluded(k.as_ref().into()),
            Bound::Unbounded => Bound::Unbounded,
        };
        // TODO: Bound::map 1.77
        let end: Bound<Slice> = match range.end_bound() {
            Bound::Included(k) => Bound::Included(k.as_ref().into()),
            Bound::Excluded(k) => Bound::Excluded(k.as_ref().into()),
            Bound::Unbounded => Bound::Unbounded,
        };
        self.cm.mark_range(keyspace.inner.id, (start, end));

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
    /// let mut tx = db.write_tx()?;
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
        &'b mut self,
        keyspace: &'b TxKeyspace,
        prefix: K,
    ) -> impl DoubleEndedIterator<Item = impl Guard + use<'b, K>> + 'b {
        self.range(keyspace, lsm_tree::range::prefix_to_range(prefix.as_ref()))
    }

    /// Inserts a key-value pair into the keyspace.
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
    /// let mut tx = db.write_tx()?;
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
        let key: UserKey = key.into();

        self.inner.insert(keyspace, key.clone(), value);
        self.cm.mark_conflict(keyspace.inner.id, key);
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
    /// let mut tx = db.write_tx()?;
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
        let key: UserKey = key.into();

        self.inner.remove(keyspace, key.clone());
        self.cm.mark_conflict(keyspace.inner.id, key);
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
    /// let mut tx = db.write_tx()?;
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
        let key: UserKey = key.into();

        self.inner.remove_weak(keyspace, key.clone());
        self.cm.mark_conflict(keyspace.inner.id, key);
    }

    /// Commits the transaction.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn commit(self) -> crate::Result<Result<(), Conflict>> {
        // NOTE: We have no write set, so we are basically
        // a read-only transaction, so nothing to do here
        if self.inner.memtables.is_empty() {
            return Ok(Ok(()));
        }

        let oracle = self.inner.db.oracle.clone();

        match oracle.with_commit(self.inner.nonce.instant, self.cm, move || {
            self.inner.commit()
        })? {
            CommitOutcome::Ok => Ok(Ok(())),
            CommitOutcome::Aborted(e) => Err(e),
            CommitOutcome::Conflicted => Ok(Err(Conflict)),
        }
    }

    /// More explicit alternative to dropping the transaction
    /// to roll it back.
    pub fn rollback(self) {
        self.inner.rollback();
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        tx::write::ssi::Conflict, KeyspaceCreateOptions, KvSeparationOptions, TxDatabase,
        TxKeyspace,
    };
    use lsm_tree::Guard;
    use tempfile::TempDir;
    use test_log::test;

    struct TestEnv {
        db: TxDatabase,
        tree: TxKeyspace,

        #[allow(unused)]
        tmpdir: TempDir,
    }

    impl TestEnv {
        fn seed_hermitage_data(&self) -> crate::Result<()> {
            self.tree.insert([1u8], [10u8])?;
            self.tree.insert([2u8], [20u8])?;
            Ok(())
        }
    }

    fn setup() -> Result<TestEnv, Box<dyn std::error::Error>> {
        let tmpdir = tempfile::tempdir()?;
        let db = TxDatabase::builder(tmpdir.path()).open()?;

        let tree = db.keyspace("foo", KeyspaceCreateOptions::default())?;

        Ok(TestEnv { db, tree, tmpdir })
    }

    // Adapted from https://github.com/al8n/skipdb/issues/10
    #[test]
    #[allow(clippy::unwrap_used)]
    fn tx_ssi_arthur_1() -> Result<(), Box<dyn std::error::Error>> {
        let env = setup()?;

        let mut tx = env.db.write_tx()?;
        tx.insert(&env.tree, "a1", 10u64.to_be_bytes());
        tx.insert(&env.tree, "b1", 100u64.to_be_bytes());
        tx.insert(&env.tree, "b2", 200u64.to_be_bytes());
        tx.commit()??;

        let mut tx1 = env.db.write_tx()?;
        let val = tx1
            .range(&env.tree, "a".."b")
            .map(|kv| {
                let v = kv.value().unwrap();

                let mut buf = [0u8; 8];
                buf.copy_from_slice(&v);
                u64::from_be_bytes(buf)
            })
            .sum::<u64>();
        tx1.insert(&env.tree, "b3", 10u64.to_be_bytes());
        assert_eq!(10, val);

        let mut tx2 = env.db.write_tx()?;
        let val = tx2
            .range(&env.tree, "b".."c")
            .map(|kv| {
                let v = kv.value().unwrap();

                let mut buf = [0u8; 8];
                buf.copy_from_slice(&v);
                u64::from_be_bytes(buf)
            })
            .sum::<u64>();
        tx2.insert(&env.tree, "a3", 300u64.to_be_bytes());
        assert_eq!(300, val);
        tx2.commit()??;
        assert!(matches!(tx1.commit()?, Err(Conflict)));

        let mut tx3 = env.db.write_tx()?;
        let val = tx3
            .iter(&env.tree)
            .filter_map(|kv| {
                let (k, v) = kv.into_inner().unwrap();

                if k.starts_with(b"a") {
                    let mut buf = [0u8; 8];
                    buf.copy_from_slice(&v);
                    Some(u64::from_be_bytes(buf))
                } else {
                    None
                }
            })
            .sum::<u64>();
        assert_eq!(310, val);

        Ok(())
    }

    // Adapted from https://github.com/al8n/skipdb/issues/10
    #[test]
    #[allow(clippy::unwrap_used)]
    fn tx_ssi_arthur_2() -> Result<(), Box<dyn std::error::Error>> {
        let env = setup()?;

        let mut tx = env.db.write_tx()?;
        tx.insert(&env.tree, "b1", 100u64.to_be_bytes());
        tx.insert(&env.tree, "b2", 200u64.to_be_bytes());
        tx.commit()??;

        let mut tx1 = env.db.write_tx()?;
        let val = tx1
            .range(&env.tree, "a".."b")
            .map(|kv| {
                let v = kv.value().unwrap();

                let mut buf = [0u8; 8];
                buf.copy_from_slice(&v);
                u64::from_be_bytes(buf)
            })
            .sum::<u64>();
        tx1.insert(&env.tree, "b3", 0u64.to_be_bytes());
        assert_eq!(0, val);

        let mut tx2 = env.db.write_tx()?;
        let val = tx2
            .range(&env.tree, "b".."c")
            .map(|kv| {
                let v = kv.value().unwrap();

                let mut buf = [0u8; 8];
                buf.copy_from_slice(&v);
                u64::from_be_bytes(buf)
            })
            .sum::<u64>();
        tx2.insert(&env.tree, "a3", 300u64.to_be_bytes());
        assert_eq!(300, val);
        tx2.commit()??;
        assert!(matches!(tx1.commit()?, Err(Conflict)));

        let mut tx3 = env.db.write_tx()?;
        let val = tx3
            .iter(&env.tree)
            .filter_map(|kv| {
                let (k, v) = kv.into_inner().unwrap();

                if k.starts_with(b"a") {
                    let mut buf = [0u8; 8];
                    buf.copy_from_slice(&v);
                    Some(u64::from_be_bytes(buf))
                } else {
                    None
                }
            })
            .sum::<u64>();
        assert_eq!(300, val);

        Ok(())
    }

    #[test]
    fn tx_ssi_basic() -> Result<(), Box<dyn std::error::Error>> {
        let env = setup()?;

        let mut tx1 = env.db.write_tx()?;
        let mut tx2 = env.db.write_tx()?;

        tx1.insert(&env.tree, "hello", "world");

        tx1.commit()??;
        assert!(env.tree.contains_key("hello")?);

        assert_eq!(tx2.get(&env.tree, "hello")?, None);

        tx2.insert(&env.tree, "hello", "world2");
        assert!(matches!(tx2.commit()?, Err(Conflict)));

        let mut tx1 = env.db.write_tx()?;
        let mut tx2 = env.db.write_tx()?;

        tx1.iter(&env.tree).next();
        tx2.insert(&env.tree, "hello", "world2");

        tx1.insert(&env.tree, "hello2", "world1");
        tx1.commit()??;

        tx2.commit()??;

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn tx_ssi_ww() -> Result<(), Box<dyn std::error::Error>> {
        // https://en.wikipedia.org/wiki/Write%E2%80%93write_conflict
        let env = setup()?;

        let mut tx1 = env.db.write_tx()?;
        let mut tx2 = env.db.write_tx()?;

        tx1.insert(&env.tree, "a", "a");
        tx2.insert(&env.tree, "b", "c");
        tx1.insert(&env.tree, "b", "b");
        tx1.commit()??;

        tx2.insert(&env.tree, "a", "c");

        tx2.commit()??;
        assert_eq!(b"c", &*env.tree.get("a")?.unwrap());
        assert_eq!(b"c", &*env.tree.get("b")?.unwrap());

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn tx_ssi_swap() -> Result<(), Box<dyn std::error::Error>> {
        let env = setup()?;

        env.tree.insert("x", "x")?;
        env.tree.insert("y", "y")?;

        let mut tx1 = env.db.write_tx()?;
        let mut tx2 = env.db.write_tx()?;

        {
            let x = tx1.get(&env.tree, "x")?.unwrap();
            tx1.insert(&env.tree, "y", x);
        }

        {
            let y = tx2.get(&env.tree, "y")?.unwrap();
            tx2.insert(&env.tree, "x", y);
        }

        tx1.commit()??;
        assert!(matches!(tx2.commit()?, Err(Conflict)));

        Ok(())
    }

    #[test]
    fn tx_ssi_write_cycles() -> Result<(), Box<dyn std::error::Error>> {
        let env = setup()?;
        env.seed_hermitage_data()?;

        let mut t1 = env.db.write_tx()?;
        let mut t2 = env.db.write_tx()?;

        t1.insert(&env.tree, [1u8], [11u8]);
        t2.insert(&env.tree, [1u8], [12u8]);
        t1.insert(&env.tree, [2u8], [21u8]);
        t1.commit()??;

        assert_eq!(env.tree.get([1u8])?, Some([11u8].into()));

        t2.insert(&env.tree, [2u8], [22u8]);
        t2.commit()??;

        assert_eq!(env.tree.get([1u8])?, Some([12u8].into()));
        assert_eq!(env.tree.get([2u8])?, Some([22u8].into()));

        Ok(())
    }

    #[test]
    fn tx_ssi_aborted_reads() -> Result<(), Box<dyn std::error::Error>> {
        let env = setup()?;
        env.seed_hermitage_data()?;

        let mut t1 = env.db.write_tx()?;
        let mut t2 = env.db.write_tx()?;

        t1.insert(&env.tree, [1u8], [101u8]);

        assert_eq!(t2.get(&env.tree, [1u8])?, Some([10u8].into()));

        t1.rollback();

        assert_eq!(t2.get(&env.tree, [1u8])?, Some([10u8].into()));

        t2.commit()??;

        Ok(())
    }

    #[allow(clippy::unwrap_used)]
    #[test]
    fn tx_ssi_anti_dependency_cycles() -> Result<(), Box<dyn std::error::Error>> {
        let env = setup()?;
        env.seed_hermitage_data()?;

        let mut t1 = env.db.write_tx()?;
        {
            let mut iter = t1.iter(&env.tree);
            assert_eq!(
                iter.next().unwrap().into_inner()?,
                ([1u8].into(), [10u8].into()),
            );
            assert_eq!(
                iter.next().unwrap().into_inner()?,
                ([2u8].into(), [20u8].into()),
            );
            assert!(iter.next().is_none());
        }

        let mut t2 = env.db.write_tx()?;
        let new = t2.update_fetch(&env.tree, [2u8], |v| {
            v.and_then(|v| v.first().copied()).map(|v| [v + 5].into())
        })?;
        assert_eq!(new, Some([25u8].into()));
        t2.commit()??;

        let mut t3 = env.db.write_tx()?;
        {
            let mut iter = t3.iter(&env.tree);
            assert_eq!(
                iter.next().unwrap().into_inner()?,
                ([1u8].into(), [10u8].into()),
            );
            assert_eq!(
                iter.next().unwrap().into_inner()?,
                ([2u8].into(), [25u8].into()),
            ); // changed here
            assert!(iter.next().is_none());
        }

        t3.commit()??;

        t1.insert(&env.tree, [1u8], [0u8]);

        assert!(matches!(t1.commit()?, Err(Conflict)));

        Ok(())
    }

    #[test]
    fn tx_ssi_update_fetch_update() -> Result<(), Box<dyn std::error::Error>> {
        let env = setup()?;

        let mut t1 = env.db.write_tx()?;
        let mut t2 = env.db.write_tx()?;

        let new = t1.update_fetch(&env.tree, "hello", |_| Some("world".into()))?;
        assert_eq!(new, Some("world".into()));
        let old = t2.fetch_update(&env.tree, "hello", |_| Some("world2".into()))?;
        assert_eq!(old, None);

        t1.commit()??;
        assert!(matches!(t2.commit()?, Err(Conflict)));

        assert_eq!(env.tree.get("hello")?, Some("world".into()));

        let mut t1 = env.db.write_tx()?;
        let mut t2 = env.db.write_tx()?;

        let old = t1.fetch_update(&env.tree, "hello", |_| Some("world3".into()))?;
        assert_eq!(old, Some("world".into()));
        let new = t2.update_fetch(&env.tree, "hello2", |_| Some("world2".into()))?;
        assert_eq!(new, Some("world2".into()));

        t1.commit()??;
        t2.commit()??;

        assert_eq!(env.tree.get("hello")?, Some("world3".into()));
        assert_eq!(env.tree.get("hello2")?, Some("world2".into()));

        Ok(())
    }

    #[test]
    fn tx_ssi_range() -> Result<(), Box<dyn std::error::Error>> {
        let env = setup()?;

        let mut t1 = env.db.write_tx()?;
        let mut t2 = env.db.write_tx()?;

        _ = t1.range(&env.tree, "h"..="hello");
        t1.insert(&env.tree, "foo", "bar");

        // insert a key INSIDE the range read by t1
        t2.insert(&env.tree, "hello", "world");

        t2.commit()??;
        assert!(matches!(t1.commit()?, Err(Conflict)));

        let mut t1 = env.db.write_tx()?;
        let mut t2 = env.db.write_tx()?;

        _ = t1.range(&env.tree, "h"..="hello");
        t1.insert(&env.tree, "foo", "bar");

        // insert a key OUTSIDE the range read by t1
        t2.insert(&env.tree, "hello2", "world");

        t2.commit()??;
        t1.commit()??;

        Ok(())
    }

    #[test]
    fn tx_ssi_prefix() -> Result<(), Box<dyn std::error::Error>> {
        let env = setup()?;

        let mut t1 = env.db.write_tx()?;
        let mut t2 = env.db.write_tx()?;

        _ = t1.prefix(&env.tree, "hello");
        t1.insert(&env.tree, "foo", "bar");

        // insert a key MATCHING the prefix read by t1
        t2.insert(&env.tree, "hello", "world");

        t2.commit()??;
        assert!(matches!(t1.commit()?, Err(Conflict)));

        let mut t1 = env.db.write_tx()?;
        let mut t2 = env.db.write_tx()?;

        _ = t1.prefix(&env.tree, "hello");
        t1.insert(&env.tree, "foo", "bar");

        // insert a key NOT MATCHING the range read by t1
        t2.insert(&env.tree, "foobar", "world");

        t2.commit()??;
        t1.commit()??;

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    #[ignore = "restore 3.0.0"]
    fn tx_ssi_gc_shadowing() -> Result<(), Box<dyn std::error::Error>> {
        let tmpdir = tempfile::tempdir()?;
        let db = TxDatabase::builder(tmpdir.path()).open()?;

        let part = db.keyspace(
            "foo",
            KeyspaceCreateOptions::default().with_kv_separation(
                KvSeparationOptions::default()
                    .separation_threshold(/* IMPORTANT: always separate */ 1),
            ),
        )?;

        part.insert("a", "a")?;
        part.inner().rotate_memtable_and_wait()?; // blob file #0

        part.insert("a", "b")?;
        part.inner().rotate_memtable_and_wait()?; // blob file #1

        // NOTE: a->a is now stale

        let mut tx = db.write_tx()?;
        tx.insert(&part, "a", "tx");

        log::info!("running GC");
        // part.gc_scan()?;
        // part.gc_with_staleness_threshold(0.0)?;
        // NOTE: The GC has now added a new value handle to the memtable
        // because a->b was written into blob file #2

        log::info!("committing tx");
        tx.commit()??;

        // NOTE: We should see the transaction's write
        assert_eq!(b"tx", &*part.get("a")?.unwrap());

        // NOTE: We should still see the transaction's write
        part.inner().rotate_memtable_and_wait()?;
        assert_eq!(b"tx", &*part.get("a")?.unwrap());

        Ok(())
    }
}
