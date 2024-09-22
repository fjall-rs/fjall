// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    batch::{item::Item, PartitionKey},
    snapshot_nonce::SnapshotNonce,
    Batch, HashMap, PersistMode, TxKeyspace, TxPartitionHandle,
};
use lsm_tree::{AbstractTree, InternalValue, KvPair, Memtable, SeqNo, UserKey, UserValue};
use std::{
    ops::{RangeBounds, RangeFull},
    sync::Arc,
};

use super::conflict_manager::BTreeCm;

fn ignore_tombstone_value(item: InternalValue) -> Option<InternalValue> {
    if item.is_tombstone() {
        None
    } else {
        Some(item)
    }
}

/// A single-writer (serialized) cross-partition transaction
///
/// Use [`WriteTransaction::commit`] to commit changes to the partition(s).
///
/// Drop the transaction to rollback changes.
pub struct WriteTransaction {
    durability: Option<PersistMode>,
    keyspace: TxKeyspace,
    memtables: HashMap<PartitionKey, Arc<Memtable>>,

    nonce: SnapshotNonce,

    conflict_manager: BTreeCm,
    read_ts: u64,
    done_read: bool,
}

impl WriteTransaction {
    pub(crate) fn new(keyspace: TxKeyspace, nonce: SnapshotNonce, read_ts: u64) -> Self {
        Self {
            keyspace,
            memtables: HashMap::default(),
            // tx_manager,
            nonce,
            durability: None,
            conflict_manager: BTreeCm::default(),
            read_ts,
            done_read: false,
        }
    }

    /// Sets the durability level.
    #[must_use]
    pub fn durability(mut self, mode: Option<PersistMode>) -> Self {
        self.durability = mode;
        self
    }

    /// Removes an item and returns its value if it existed.
    ///
    /// The operation will run wrapped in a transaction.
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionCreateOptions};
    /// # use std::sync::Arc;
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open_transactional()?;
    /// # let partition = keyspace.open_partition("default", PartitionCreateOptions::default())?;
    /// partition.insert("a", "abc")?;
    ///
    /// let mut tx = keyspace.write_tx();
    ///
    /// let taken = tx.take(&partition, "a")?.unwrap();
    /// assert_eq!(b"abc", &*taken);
    /// tx.commit()?;
    ///
    /// let item = partition.get("a")?;
    /// assert!(item.is_none());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn take<K: AsRef<[u8]>>(
        &mut self,
        partition: &TxPartitionHandle,
        key: K,
    ) -> crate::Result<Option<UserValue>> {
        self.fetch_update(partition, key, |_| None)
    }

    /// Atomically updates an item and returns the new value.
    ///
    /// Returning `None` removes the item if it existed before.
    ///
    /// The operation will run wrapped in a transaction.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionCreateOptions, Slice};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open_transactional()?;
    /// # let partition = keyspace.open_partition("default", PartitionCreateOptions::default())?;
    /// partition.insert("a", "abc")?;
    ///
    /// let mut tx = keyspace.write_tx();
    ///
    /// let updated = tx.update_fetch(&partition, "a", |_| Some(Slice::from(*b"def")))?.unwrap();
    /// assert_eq!(b"def", &*updated);
    /// tx.commit()?;
    ///
    /// let item = partition.get("a")?;
    /// assert_eq!(Some("def".as_bytes().into()), item);
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionCreateOptions};
    /// # use std::sync::Arc;
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open_transactional()?;
    /// # let partition = keyspace.open_partition("default", PartitionCreateOptions::default())?;
    /// partition.insert("a", "abc")?;
    ///
    /// let mut tx = keyspace.write_tx();
    ///
    /// let updated = tx.update_fetch(&partition, "a", |_| None)?;
    /// assert!(updated.is_none());
    /// tx.commit()?;
    ///
    /// let item = partition.get("a")?;
    /// assert!(item.is_none());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn update_fetch<K: AsRef<[u8]>, F: Fn(Option<&UserValue>) -> Option<UserValue>>(
        &mut self,
        partition: &TxPartitionHandle,
        key: K,
        f: F,
    ) -> crate::Result<Option<UserValue>> {
        let prev = self.get(partition, &key)?;
        let updated = f(prev.as_ref());

        self.conflict_manager
            .mark_read(&partition.inner.name, &key.as_ref().into());
        self.conflict_manager
            .mark_conflict(&partition.inner.name, &key.as_ref().into());

        if let Some(value) = &updated {
            self.insert(partition, &key, value);
        } else if prev.is_some() {
            self.remove(partition, &key);
        }

        Ok(updated)
    }

    /// Atomically updates an item and returns the previous value.
    ///
    /// Returning `None` removes the item if it existed before.
    ///
    /// The operation will run wrapped in a transaction.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionCreateOptions, Slice};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open_transactional()?;
    /// # let partition = keyspace.open_partition("default", PartitionCreateOptions::default())?;
    /// partition.insert("a", "abc")?;
    ///
    /// let mut tx = keyspace.write_tx();
    ///
    /// let prev = tx.fetch_update(&partition, "a", |_| Some(Slice::from(*b"def")))?.unwrap();
    /// assert_eq!(b"abc", &*prev);
    /// tx.commit()?;
    ///
    /// let item = partition.get("a")?;
    /// assert_eq!(Some("def".as_bytes().into()), item);
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionCreateOptions};
    /// # use std::sync::Arc;
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open_transactional()?;
    /// # let partition = keyspace.open_partition("default", PartitionCreateOptions::default())?;
    /// partition.insert("a", "abc")?;
    ///
    /// let mut tx = keyspace.write_tx();
    ///
    /// let prev = tx.fetch_update(&partition, "a", |_| None)?.unwrap();
    /// assert_eq!(b"abc", &*prev);
    /// tx.commit()?;
    ///
    /// let item = partition.get("a")?;
    /// assert!(item.is_none());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn fetch_update<K: AsRef<[u8]>, F: Fn(Option<&UserValue>) -> Option<UserValue>>(
        &mut self,
        partition: &TxPartitionHandle,
        key: K,
        f: F,
    ) -> crate::Result<Option<UserValue>> {
        let prev = self.get(partition, &key)?;
        let updated = f(prev.as_ref());

        self.conflict_manager
            .mark_read(&partition.inner.name, &key.as_ref().into());
        self.conflict_manager
            .mark_conflict(&partition.inner.name, &key.as_ref().into());

        if let Some(value) = updated {
            self.insert(partition, &key, value);
        } else if prev.is_some() {
            self.remove(partition, &key);
        }

        Ok(prev)
    }

    /// Retrieves an item from the transaction's state.
    ///
    /// The transaction allows reading your own writes (RYOW).
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open_transactional()?;
    /// # let partition = keyspace.open_partition("default", PartitionCreateOptions::default())?;
    /// partition.insert("a", "previous_value")?;
    /// assert_eq!(b"previous_value", &*partition.get("a")?.unwrap());
    ///
    /// let mut tx = keyspace.write_tx();
    /// tx.insert(&partition, "a", "new_value");
    ///
    /// // Read-your-own-write
    /// let item = tx.get(&partition, "a")?;
    /// assert_eq!(Some("new_value".as_bytes().into()), item);
    ///
    /// drop(tx);
    ///
    /// // Write was not committed
    /// assert_eq!(b"previous_value", &*partition.get("a")?.unwrap());
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
        if let Some(memtable) = self.memtables.get(&partition.inner.name) {
            if let Some(item) = memtable.get(&key, None) {
                return Ok(ignore_tombstone_value(item).map(|x| x.value));
            }
        }

        let res = partition
            .inner
            .snapshot_at(self.nonce.instant)
            .get(key.as_ref())?;

        self.conflict_manager
            .mark_read(&partition.inner.name, &key.as_ref().into());

        Ok(res)
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
    /// assert!(keyspace.read_tx().contains_key(&partition, "a")?);
    ///
    /// let mut tx = keyspace.write_tx();
    /// assert!(tx.contains_key(&partition, "a")?);
    ///
    /// tx.insert(&partition, "b", "my_value2");
    /// assert!(tx.contains_key(&partition, "b")?);
    ///
    /// // Transaction not committed yet
    /// assert!(!keyspace.read_tx().contains_key(&partition, "b")?);
    ///
    /// tx.commit()?;
    /// assert!(keyspace.read_tx().contains_key(&partition, "b")?);
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
        if let Some(memtable) = self.memtables.get(&partition.inner.name) {
            if let Some(item) = memtable.get(&key, None) {
                return Ok(!item.key.is_tombstone());
            }
        }

        partition
            .inner
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
    /// #
    /// let mut tx = keyspace.write_tx();
    /// tx.insert(&partition, "1", "abc");
    /// tx.insert(&partition, "3", "abc");
    /// tx.insert(&partition, "5", "abc");
    ///
    /// let (key, _) = tx.first_key_value(&partition)?.expect("item should exist");
    /// assert_eq!(&*key, "1".as_bytes());
    ///
    /// assert!(keyspace.read_tx().first_key_value(&partition)?.is_none());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn first_key_value(&self, partition: &TxPartitionHandle) -> crate::Result<Option<KvPair>> {
        // TODO: calling .iter will mark the partition as fully read, is that what we want?
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
    /// #
    /// let mut tx = keyspace.write_tx();
    /// tx.insert(&partition, "1", "abc");
    /// tx.insert(&partition, "3", "abc");
    /// tx.insert(&partition, "5", "abc");
    ///
    /// let (key, _) = tx.last_key_value(&partition)?.expect("item should exist");
    /// assert_eq!(&*key, "5".as_bytes());
    ///
    /// assert!(keyspace.read_tx().last_key_value(&partition)?.is_none());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn last_key_value(&self, partition: &TxPartitionHandle) -> crate::Result<Option<KvPair>> {
        // TODO: calling .iter will mark the partition as fully read, is that what we want?
        self.iter(partition).next_back().transpose()
    }

    /// Scans the entire partition, returning the amount of items.
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
    /// let mut tx = keyspace.write_tx();
    /// assert_eq!(2, tx.len(&partition)?);
    ///
    /// tx.insert(&partition, "c", "my_value3");
    ///
    /// // read-your-own write
    /// assert_eq!(3, tx.len(&partition)?);
    ///
    /// // Transaction is not committed yet
    /// assert_eq!(2, keyspace.read_tx().len(&partition)?);
    ///
    /// tx.commit()?;
    /// assert_eq!(3, keyspace.read_tx().len(&partition)?);
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn len(&self, partition: &TxPartitionHandle) -> crate::Result<usize> {
        let mut count = 0;

        // TODO: calling .iter will mark the partition as fully read, is that what we want?
        let iter = self.iter(partition);

        for kv in iter {
            let _ = kv?;
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
    /// # use fjall::{Config, Keyspace, PartitionCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open_transactional()?;
    /// # let partition = keyspace.open_partition("default", PartitionCreateOptions::default())?;
    /// #
    /// let mut tx = keyspace.write_tx();
    /// tx.insert(&partition, "a", "abc");
    /// tx.insert(&partition, "f", "abc");
    /// tx.insert(&partition, "g", "abc");
    ///
    /// assert_eq!(3, tx.iter(&partition).count());
    /// assert_eq!(0, keyspace.read_tx().iter(&partition).count());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    #[must_use]
    pub fn iter(
        &self,
        partition: &TxPartitionHandle,
    ) -> impl DoubleEndedIterator<Item = crate::Result<KvPair>> + 'static {
        self.conflict_manager
            .mark_range(&partition.inner.name, RangeFull);
        partition
            .inner
            .tree
            .iter_with_seqno(
                self.nonce.instant,
                self.memtables.get(&partition.inner.name).cloned(),
            )
            .map(|item| item.map_err(Into::into))
    }

    /// Iterates over the transaction's state, returning keys only.
    ///
    /// Avoid using this function, or limit it as otherwise it may scan a lot of items.
    #[must_use]
    pub fn keys(
        &self,
        partition: &TxPartitionHandle,
    ) -> impl DoubleEndedIterator<Item = crate::Result<UserKey>> {
        // TODO: is that what we want here? we're only reading keys, but then again...
        self.conflict_manager
            .mark_range(&partition.inner.name, RangeFull);
        partition
            .inner
            .tree
            .keys_with_seqno(self.nonce.instant, None)
            .map(|item| item.map_err(Into::into))
    }

    /// Iterates over the transaction's state, returning values only.
    ///
    /// Avoid using this function, or limit it as otherwise it may scan a lot of items.
    #[must_use]
    pub fn values(
        &self,
        partition: &TxPartitionHandle,
    ) -> impl DoubleEndedIterator<Item = crate::Result<UserValue>> {
        self.conflict_manager
            .mark_range(&partition.inner.name, RangeFull);
        partition
            .inner
            .tree
            .values_with_seqno(self.nonce.instant, None)
            .map(|item| item.map_err(Into::into))
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
    /// #
    /// let mut tx = keyspace.write_tx();
    /// tx.insert(&partition, "a", "abc");
    /// tx.insert(&partition, "f", "abc");
    /// tx.insert(&partition, "g", "abc");
    ///
    /// assert_eq!(2, tx.range(&partition, "a"..="f").count());
    /// assert_eq!(0, keyspace.read_tx().range(&partition, "a"..="f").count());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    #[must_use]
    pub fn range<'b, K: AsRef<[u8]> + 'b, R: RangeBounds<K> + 'b>(
        &'b self,
        partition: &'b TxPartitionHandle,
        range: R,
    ) -> impl DoubleEndedIterator<Item = crate::Result<KvPair>> + 'static {
        self.conflict_manager.mark_range(
            &partition.inner.name,
            (
                range
                    .start_bound()
                    .map(|start| start.as_ref().into())
                    .as_ref(),
                range.end_bound().map(|end| end.as_ref().into()).as_ref(),
            ),
        );
        partition
            .inner
            .tree
            .range_with_seqno(
                range,
                self.nonce.instant,
                self.memtables.get(&partition.inner.name).cloned(),
            )
            .map(|item| item.map_err(Into::into))
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
    /// #
    /// let mut tx = keyspace.write_tx();
    /// tx.insert(&partition, "a", "abc");
    /// tx.insert(&partition, "ab", "abc");
    /// tx.insert(&partition, "abc", "abc");
    ///
    /// assert_eq!(2, tx.prefix(&partition, "ab").count());
    /// assert_eq!(0, keyspace.read_tx().prefix(&partition, "ab").count());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    #[must_use]
    pub fn prefix<'b, K: AsRef<[u8]> + 'b>(
        &'b self,
        partition: &'b TxPartitionHandle,
        prefix: K,
    ) -> impl DoubleEndedIterator<Item = crate::Result<KvPair>> + 'static {
        self.conflict_manager
            .mark_prefix(&partition.inner.name, prefix.as_ref().into());
        partition
            .inner
            .tree
            .prefix_with_seqno(
                prefix,
                self.nonce.instant,
                self.memtables.get(&partition.inner.name).cloned(),
            )
            .map(|item| item.map_err(Into::into))
    }

    /// Inserts a key-value pair into the partition.
    ///
    /// Keys may be up to 65536 bytes long, values up to 2^32 bytes.
    /// Shorter keys and values result in better performance.
    ///
    /// If the key already exists, the item will be overwritten.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open_transactional()?;
    /// # let partition = keyspace.open_partition("default", PartitionCreateOptions::default())?;
    /// partition.insert("a", "previous_value")?;
    /// assert_eq!(b"previous_value", &*partition.get("a")?.unwrap());
    ///
    /// let mut tx = keyspace.write_tx();
    /// tx.insert(&partition, "a", "new_value");
    ///
    /// drop(tx);
    ///
    /// // Write was not committed
    /// assert_eq!(b"previous_value", &*partition.get("a")?.unwrap());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn insert<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        &mut self,
        partition: &TxPartitionHandle,
        key: K,
        value: V,
    ) {
        self.memtables
            .entry(partition.inner.name.clone())
            .or_default()
            .insert(lsm_tree::InternalValue::from_components(
                key.as_ref(),
                value.as_ref(),
                // NOTE: Just take the max seqno, which should never be reached
                // that way, the write is definitely always the newest
                SeqNo::MAX,
                lsm_tree::ValueType::Value,
            ));
        self.conflict_manager
            .mark_conflict(&partition.inner.name, &key.as_ref().into());
    }

    /// Removes an item from the partition.
    ///
    /// The key may be up to 65536 bytes long.
    /// Shorter keys result in better performance.
    ///
    /// # Examples
    ///
    /// ```
    /// # use fjall::{Config, Keyspace, PartitionCreateOptions};
    /// #
    /// # let folder = tempfile::tempdir()?;
    /// # let keyspace = Config::new(folder).open_transactional()?;
    /// # let partition = keyspace.open_partition("default", PartitionCreateOptions::default())?;
    /// partition.insert("a", "previous_value")?;
    /// assert_eq!(b"previous_value", &*partition.get("a")?.unwrap());
    ///
    /// let mut tx = keyspace.write_tx();
    /// tx.remove(&partition, "a");
    ///
    /// // Read-your-own-write
    /// let item = tx.get(&partition, "a")?;
    /// assert_eq!(None, item);
    ///
    /// drop(tx);
    ///
    /// // Deletion was not committed
    /// assert_eq!(b"previous_value", &*partition.get("a")?.unwrap());
    /// #
    /// # Ok::<(), fjall::Error>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn remove<K: AsRef<[u8]>>(&mut self, partition: &TxPartitionHandle, key: K) {
        self.memtables
            .entry(partition.inner.name.clone())
            .or_default()
            .insert(lsm_tree::InternalValue::new_tombstone(
                key.as_ref(),
                // NOTE: Just take the max seqno, which should never be reached
                // that way, the write is definitely always the newest
                SeqNo::MAX,
            ));
        self.conflict_manager
            .mark_conflict(&partition.inner.name, &key.as_ref().into());
    }

    /// Commits the transaction.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn commit(mut self) -> crate::Result<()> {
        // skip all the logic if no keys were written to
        if self.memtables.is_empty() {
            return Ok(());
        }

        let _write_lock = self
            .keyspace
            .orc
            .write_serialize_lock
            .lock()
            .expect("write serialization lock is poisoned");

        let cm_res = self.keyspace.orc.new_commit_ts(
            &mut self.done_read,
            self.read_ts,
            self.conflict_manager.into(),
        );

        let commit_ts = match cm_res {
            super::oracle::CreateCommitTimestampResult::Timestamp(ts) => ts,
            super::oracle::CreateCommitTimestampResult::Conflict(_conflict_manager) => {
                return Err(crate::Error::Conflict);
            }
        };

        let mut batch = Batch::new(self.keyspace.inner).durability(self.durability);

        /*
        for (partition_key, memtable) in self.memtables {
            let memtable = Arc::into_inner(memtable).expect("should be able to unwrap Arc");

            for (internal_key, value) in memtable.items {
                batch.data.push(Item::new(
                    partition_key.clone(),
                    internal_key.user_key,
                    value,
                    internal_key.value_type,
                ));
            }
        }
        */

        for (partition_key, memtable) in self.memtables {
            for item in memtable.iter() {
                batch.data.push(Item::new(
                    partition_key.clone(),
                    item.key.user_key.clone(),
                    item.value.clone(),
                    item.key.value_type,
                ));
            }
        }

        // TODO: instead of using batch, write batch::commit as a generic function that takes
        // a impl Iterator<BatchItem>
        // that way, we don't have to move the memtable(s) into the batch first to commit
        batch.commit()?;

        self.keyspace.orc.done_commit(commit_ts);

        Ok(())
    }

    /// More explicit alternative to dropping the transaction
    /// to roll it back.
    pub fn rollback(self) {}
}

#[cfg(test)]
mod tests {

    use crate::{Config, PartitionCreateOptions};

    #[test]
    fn basic_tx_test() {
        let tmpdir = tempfile::tempdir().unwrap();
        let keyspace = Config::new(tmpdir.path()).open_transactional().unwrap();

        let part = keyspace
            .open_partition("foo", PartitionCreateOptions::default())
            .unwrap();

        let mut tx1 = keyspace.write_tx();
        let mut tx2 = keyspace.write_tx();

        tx1.insert(&part, "hello", "world");

        tx1.commit().unwrap();
        assert!(part.contains_key("hello").unwrap());

        assert_eq!(tx2.get(&part, "hello").unwrap(), None);

        tx2.insert(&part, "hello", "world2");
        assert!(matches!(tx2.commit(), Err(crate::Error::Conflict)));

        let mut tx1 = keyspace.write_tx();
        let mut tx2 = keyspace.write_tx();

        tx1.iter(&part).next();
        tx2.insert(&part, "hello", "world2");

        tx1.insert(&part, "hello2", "world1");
        tx1.commit().unwrap();

        tx2.commit().unwrap();
    }

    #[test]
    fn tx_ssi_swap() -> crate::Result<()> {
        let tmpdir = tempfile::tempdir().unwrap();
        let keyspace = Config::new(tmpdir.path()).open_transactional().unwrap();

        let part = keyspace
            .open_partition("foo", PartitionCreateOptions::default())
            .unwrap();

        part.insert("x", "x")?;
        part.insert("y", "y")?;

        let mut tx1 = keyspace.write_tx();
        let mut tx2 = keyspace.write_tx();

        {
            let x = tx1.get(&part, "x")?.unwrap();
            tx1.insert(&part, "y", x);
        }

        {
            let y = tx2.get(&part, "y")?.unwrap();
            tx2.insert(&part, "x", y);
        }

        tx1.commit().unwrap();
        assert!(matches!(tx2.commit(), Err(crate::Error::Conflict)));

        Ok(())
    }
}
