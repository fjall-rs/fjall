// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    snapshot_nonce::SnapshotNonce,
    tx::{
        optimistic::{
            conflict_manager::ConflictManager,
            oracle::{CommitOutcome, Oracle},
        },
        write_tx::BaseTransaction,
    },
    Database, Guard, Iter, Keyspace, PersistMode, Readable,
};
use lsm_tree::{KvPair, Slice, UserKey, UserValue};
use std::{
    fmt,
    ops::{Bound, RangeBounds, RangeFull},
    sync::Arc,
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

/// A cross-keyspace transaction using optimistic concurrency control
///
/// Use [`WriteTransaction::commit`] to commit changes to the keyspace(s).
///
/// Transactions keep a consistent view of the database at the time,
/// meaning old data will not be dropped until it is not referenced by any active transaction.
///
/// For that reason, you should try to keep transactions short-lived, and make sure they
/// are not held somewhere forever.
///
/// # Caution
///
/// The transaction may fail and have to be rerun if it conflicts.
#[clippy::has_significant_drop]
pub struct WriteTransaction {
    inner: BaseTransaction,
    cm: ConflictManager,
    oracle: Arc<Oracle>,
}

impl Readable for WriteTransaction {
    fn get<K: AsRef<[u8]>>(
        &self,
        keyspace: impl AsRef<Keyspace>,
        key: K,
    ) -> crate::Result<Option<UserValue>> {
        let keyspace = keyspace.as_ref();

        let res = self.inner.get(keyspace, key.as_ref())?;

        self.cm.mark_read(keyspace.id, key.as_ref().into());

        Ok(res)
    }

    fn contains_key<K: AsRef<[u8]>>(
        &self,
        keyspace: impl AsRef<Keyspace>,
        key: K,
    ) -> crate::Result<bool> {
        let keyspace = keyspace.as_ref();

        let contains = self.inner.contains_key(keyspace, key.as_ref())?;

        self.cm.mark_read(keyspace.id, key.as_ref().into());

        Ok(contains)
    }

    fn first_key_value(&self, keyspace: impl AsRef<Keyspace>) -> Option<Guard> {
        self.iter(&keyspace).next()
    }

    fn last_key_value(&self, keyspace: impl AsRef<Keyspace>) -> Option<Guard> {
        self.iter(&keyspace).next_back()
    }

    fn size_of<K: AsRef<[u8]>>(
        &self,
        keyspace: impl AsRef<Keyspace>,
        key: K,
    ) -> crate::Result<Option<u32>> {
        let keyspace = keyspace.as_ref();
        self.inner.size_of(keyspace, key)
    }

    fn iter(&self, keyspace: impl AsRef<Keyspace>) -> Iter {
        self.cm.mark_range(keyspace.as_ref().id, RangeFull);
        self.inner.iter(keyspace)
    }

    fn range<K: AsRef<[u8]>, R: RangeBounds<K>>(
        &self,
        keyspace: impl AsRef<Keyspace>,
        range: R,
    ) -> Iter {
        let start: Bound<Slice> = range.start_bound().map(|k| k.as_ref().into());
        let end: Bound<Slice> = range.end_bound().map(|k| k.as_ref().into());

        self.cm.mark_range(keyspace.as_ref().id, (start, end));

        self.inner.range(keyspace, range)
    }

    fn prefix<K: AsRef<[u8]>>(&self, keyspace: impl AsRef<Keyspace>, prefix: K) -> Iter {
        self.range(keyspace, lsm_tree::range::prefix_to_range(prefix.as_ref()))
    }
}

impl WriteTransaction {
    pub(crate) fn new(db: Database, nonce: SnapshotNonce, oracle: Arc<Oracle>) -> Self {
        Self {
            inner: BaseTransaction::new(db, nonce),
            cm: ConflictManager::default(),
            oracle,
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
    /// # use fjall::{OptimisticTxDatabase, KeyspaceCreateOptions, Readable};
    /// # use std::sync::Arc;
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = OptimisticTxDatabase::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
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
        keyspace: impl AsRef<Keyspace>,
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
    /// # use fjall::{OptimisticTxDatabase, KeyspaceCreateOptions, Readable, Slice};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = OptimisticTxDatabase::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
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
    /// # use fjall::{OptimisticTxDatabase, KeyspaceCreateOptions, Readable};
    /// # use std::sync::Arc;
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = OptimisticTxDatabase::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
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
        keyspace: impl AsRef<Keyspace>,
        key: K,
        f: F,
    ) -> crate::Result<Option<UserValue>> {
        let keyspace = keyspace.as_ref();
        let key: UserKey = key.into();

        let updated = self.inner.update_fetch(keyspace, key.clone(), f)?;

        self.cm.mark_read(keyspace.id, key.clone());
        self.cm.mark_conflict(keyspace.id, key);

        Ok(updated)
    }

    /// Atomically updates an item and returns the previous value.
    ///
    /// Returning `None` removes the item if it existed before.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{OptimisticTxDatabase, KeyspaceCreateOptions, Readable, Slice};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = OptimisticTxDatabase::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
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
    /// # use fjall::{OptimisticTxDatabase, KeyspaceCreateOptions, Readable};
    /// # use std::sync::Arc;
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = OptimisticTxDatabase::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
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
        keyspace: impl AsRef<Keyspace>,
        key: K,
        f: F,
    ) -> crate::Result<Option<UserValue>> {
        let keyspace = keyspace.as_ref();
        let key = key.into();

        let prev = self.inner.fetch_update(keyspace, key.clone(), f)?;

        self.cm.mark_read(keyspace.id, key.clone());
        self.cm.mark_conflict(keyspace.id, key);

        Ok(prev)
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
    /// # use fjall::{OptimisticTxDatabase, KeyspaceCreateOptions, Readable};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = OptimisticTxDatabase::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
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
        keyspace: impl AsRef<Keyspace>,
        key: K,
        value: V,
    ) {
        let keyspace = keyspace.as_ref();
        let key: UserKey = key.into();

        self.inner.insert(keyspace, key.clone(), value);
        self.cm.mark_conflict(keyspace.id, key);
    }

    /// Removes an item from the keyspace.
    ///
    /// The key may be up to 65536 bytes long.
    /// Shorter keys result in better performance.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{OptimisticTxDatabase, KeyspaceCreateOptions, Readable};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = OptimisticTxDatabase::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
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
    pub fn remove<K: Into<UserKey>>(&mut self, keyspace: impl AsRef<Keyspace>, key: K) {
        let keyspace = keyspace.as_ref();
        let key: UserKey = key.into();

        self.inner.remove(keyspace, key.clone());
        self.cm.mark_conflict(keyspace.id, key);
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
    /// # use fjall::{OptimisticTxDatabase, KeyspaceCreateOptions, Readable};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let db = OptimisticTxDatabase::builder(folder).open()?;
    /// # let tree = db.keyspace("default", KeyspaceCreateOptions::default)?;
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
    pub fn remove_weak<K: Into<UserKey>>(&mut self, keyspace: impl AsRef<Keyspace>, key: K) {
        let keyspace = keyspace.as_ref();
        let key: UserKey = key.into();

        self.inner.remove_weak(keyspace, key.clone());
        self.cm.mark_conflict(keyspace.id, key);
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

        let oracle = self.oracle.clone();

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
        Conflict, KeyspaceCreateOptions, OptimisticTxDatabase, OptimisticTxKeyspace, Readable,
    };
    use tempfile::TempDir;
    use test_log::test;

    struct TestEnv {
        db: OptimisticTxDatabase,
        tree: OptimisticTxKeyspace,

        #[expect(unused)]
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
        let db = OptimisticTxDatabase::builder(tmpdir.path()).open()?;

        let tree = db.keyspace("foo", KeyspaceCreateOptions::default)?;

        Ok(TestEnv { db, tree, tmpdir })
    }

    // Adapted from https://github.com/al8n/skipdb/issues/10
    #[test]
    #[expect(clippy::unwrap_used)]
    fn tx_ssi_arthur_1() -> Result<(), Box<dyn std::error::Error>> {
        let env = setup()?;

        let mut tx = env.db.write_tx()?;
        tx.insert(env.tree.inner(), "a1", 10u64.to_be_bytes());
        tx.insert(env.tree.inner(), "b1", 100u64.to_be_bytes());
        tx.insert(env.tree.inner(), "b2", 200u64.to_be_bytes());
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
        tx1.insert(env.tree.inner(), "b3", 10u64.to_be_bytes());
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
        tx2.insert(env.tree.inner(), "a3", 300u64.to_be_bytes());
        assert_eq!(300, val);
        tx2.commit()??;
        assert!(matches!(tx1.commit()?, Err(Conflict)));

        let tx3 = env.db.write_tx()?;
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
    #[expect(clippy::unwrap_used)]
    fn tx_ssi_arthur_2() -> Result<(), Box<dyn std::error::Error>> {
        let env = setup()?;

        let mut tx = env.db.write_tx()?;
        tx.insert(env.tree.inner(), "b1", 100u64.to_be_bytes());
        tx.insert(env.tree.inner(), "b2", 200u64.to_be_bytes());
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
        tx1.insert(env.tree.inner(), "b3", 0u64.to_be_bytes());
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
        tx2.insert(env.tree.inner(), "a3", 300u64.to_be_bytes());
        assert_eq!(300, val);
        tx2.commit()??;
        assert!(matches!(tx1.commit()?, Err(Conflict)));

        let tx3 = env.db.write_tx()?;
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

        tx1.insert(env.tree.inner(), "hello", "world");

        tx1.commit()??;
        assert!(env.tree.contains_key("hello")?);

        assert_eq!(tx2.get(env.tree.inner(), "hello")?, None);

        tx2.insert(env.tree.inner(), "hello", "world2");
        assert!(matches!(tx2.commit()?, Err(Conflict)));

        let mut tx1 = env.db.write_tx()?;
        let mut tx2 = env.db.write_tx()?;

        tx1.iter(&env.tree).next();
        tx2.insert(env.tree.inner(), "hello", "world2");

        tx1.insert(env.tree.inner(), "hello2", "world1");
        tx1.commit()??;

        tx2.commit()??;

        Ok(())
    }

    #[test]
    #[expect(clippy::unwrap_used)]
    fn tx_ssi_ww() -> Result<(), Box<dyn std::error::Error>> {
        // https://en.wikipedia.org/wiki/Write%E2%80%93write_conflict
        let env = setup()?;

        let mut tx1 = env.db.write_tx()?;
        let mut tx2 = env.db.write_tx()?;

        tx1.insert(env.tree.inner(), "a", "a");
        tx2.insert(env.tree.inner(), "b", "c");
        tx1.insert(env.tree.inner(), "b", "b");
        tx1.commit()??;

        tx2.insert(env.tree.inner(), "a", "c");

        tx2.commit()??;
        assert_eq!(b"c", &*env.tree.get("a")?.unwrap());
        assert_eq!(b"c", &*env.tree.get("b")?.unwrap());

        Ok(())
    }

    #[test]
    #[expect(clippy::unwrap_used)]
    fn tx_ssi_swap() -> Result<(), Box<dyn std::error::Error>> {
        let env = setup()?;

        env.tree.insert("x", "x")?;
        env.tree.insert("y", "y")?;

        let mut tx1 = env.db.write_tx()?;
        let mut tx2 = env.db.write_tx()?;

        {
            let x = tx1.get(env.tree.inner(), "x")?.unwrap();
            tx1.insert(env.tree.inner(), "y", x);
        }

        {
            let y = tx2.get(env.tree.inner(), "y")?.unwrap();
            tx2.insert(env.tree.inner(), "x", y);
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

        t1.insert(env.tree.inner(), [1u8], [11u8]);
        t2.insert(env.tree.inner(), [1u8], [12u8]);
        t1.insert(env.tree.inner(), [2u8], [21u8]);
        t1.commit()??;

        assert_eq!(env.tree.get([1u8])?, Some([11u8].into()));

        t2.insert(env.tree.inner(), [2u8], [22u8]);
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
        let t2 = env.db.write_tx()?;

        t1.insert(env.tree.inner(), [1u8], [101u8]);

        assert_eq!(t2.get(env.tree.inner(), [1u8])?, Some([10u8].into()));

        t1.rollback();

        assert_eq!(t2.get(env.tree.inner(), [1u8])?, Some([10u8].into()));

        t2.commit()??;

        Ok(())
    }

    #[expect(clippy::unwrap_used)]
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

        let t3 = env.db.write_tx()?;
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

        t1.insert(env.tree.inner(), [1u8], [0u8]);

        assert!(matches!(t1.commit()?, Err(Conflict)));

        Ok(())
    }

    #[test]
    fn tx_ssi_update_fetch_update() -> Result<(), Box<dyn std::error::Error>> {
        let env = setup()?;

        let mut t1 = env.db.write_tx()?;
        let mut t2 = env.db.write_tx()?;

        let new = t1.update_fetch(env.tree.inner(), "hello", |_| Some("world".into()))?;
        assert_eq!(new, Some("world".into()));
        let old = t2.fetch_update(env.tree.inner(), "hello", |_| Some("world2".into()))?;
        assert_eq!(old, None);

        t1.commit()??;
        assert!(matches!(t2.commit()?, Err(Conflict)));

        assert_eq!(env.tree.get("hello")?, Some("world".into()));

        let mut t1 = env.db.write_tx()?;
        let mut t2 = env.db.write_tx()?;

        let old = t1.fetch_update(env.tree.inner(), "hello", |_| Some("world3".into()))?;
        assert_eq!(old, Some("world".into()));
        let new = t2.update_fetch(env.tree.inner(), "hello2", |_| Some("world2".into()))?;
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
        t1.insert(env.tree.inner(), "foo", "bar");

        // insert a key INSIDE the range read by t1
        t2.insert(env.tree.inner(), "hello", "world");

        t2.commit()??;
        assert!(matches!(t1.commit()?, Err(Conflict)));

        let mut t1 = env.db.write_tx()?;
        let mut t2 = env.db.write_tx()?;

        _ = t1.range(&env.tree, "h"..="hello");
        t1.insert(env.tree.inner(), "foo", "bar");

        // insert a key OUTSIDE the range read by t1
        t2.insert(env.tree.inner(), "hello2", "world");

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
        t1.insert(env.tree.inner(), "foo", "bar");

        // insert a key MATCHING the prefix read by t1
        t2.insert(env.tree.inner(), "hello", "world");

        t2.commit()??;
        assert!(matches!(t1.commit()?, Err(Conflict)));

        let mut t1 = env.db.write_tx()?;
        let mut t2 = env.db.write_tx()?;

        _ = t1.prefix(&env.tree, "hello");
        t1.insert(env.tree.inner(), "foo", "bar");

        // insert a key NOT MATCHING the range read by t1
        t2.insert(env.tree.inner(), "foobar", "world");

        t2.commit()??;
        t1.commit()??;

        Ok(())
    }
}
