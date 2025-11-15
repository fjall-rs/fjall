// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    batch::item::Item, keyspace::InternalKeyspaceId, snapshot_nonce::SnapshotNonce, Database,
    Guard, HashMap, Keyspace, PersistMode, WriteBatch,
};
use lsm_tree::{AbstractTree, InternalValue, KvPair, Memtable, SeqNo, UserKey, UserValue};
use std::{ops::RangeBounds, sync::Arc};

fn ignore_tombstone_value(item: InternalValue) -> Option<InternalValue> {
    if item.is_tombstone() {
        None
    } else {
        Some(item)
    }
}

pub(super) struct BaseTransaction {
    /// Database to work with
    pub(super) db: Database,

    /// Ephemeral transaction changes
    ///
    /// Used for RYOW (read-your-own-writes)
    pub(crate) memtables: HashMap<InternalKeyspaceId, Arc<Memtable>>,

    /// The snapshot, for repeatable reads
    pub(crate) nonce: SnapshotNonce,

    /// Durability level used, see [`PersistMode`].
    pub(crate) durability: Option<PersistMode>,

    /// Current sequence number
    ///
    /// The sequence number starts at 0b1000...0000 and is incremented with each write.
    ///
    /// This ensures that writes within the transaction are always newer than any existing data.
    pub(crate) seqno: SeqNo,
}

impl BaseTransaction {
    pub(crate) fn new(db: Database, nonce: SnapshotNonce) -> Self {
        Self {
            db,
            memtables: HashMap::default(),
            nonce,
            durability: None,
            seqno: 0x8000_0000_0000_0000,
        }
    }

    /// Sets the durability level.
    #[must_use]
    pub(super) fn durability(mut self, mode: Option<PersistMode>) -> Self {
        self.durability = mode;
        self
    }

    /// Removes an item and returns its value if it existed.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub(super) fn take<K: Into<UserKey>>(
        &mut self,
        keyspace: &Keyspace,
        key: K,
    ) -> crate::Result<Option<UserValue>> {
        self.fetch_update(keyspace, key, |_| None)
    }

    /// Atomically updates an item and returns the new value.
    ///
    /// Returning `None` removes the item if it existed before.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub(super) fn update_fetch<
        K: Into<UserKey>,
        F: FnMut(Option<&UserValue>) -> Option<UserValue>,
    >(
        &mut self,
        keyspace: &Keyspace,
        key: K,
        mut f: F,
    ) -> crate::Result<Option<UserValue>> {
        let key = key.into();
        let prev = self.get(keyspace, &key)?;
        let updated = f(prev.as_ref());

        if let Some(value) = updated.clone() {
            // NOTE: Skip insert if the value hasn't changed
            if prev.as_ref() != Some(&value) {
                self.insert(keyspace, key, value);
            }
        } else if prev.is_some() {
            self.remove(keyspace, key);
        }

        Ok(updated)
    }

    /// Atomically updates an item and returns the previous value.
    ///
    /// Returning `None` removes the item if it existed before.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub(super) fn fetch_update<
        K: Into<UserKey>,
        F: FnMut(Option<&UserValue>) -> Option<UserValue>,
    >(
        &mut self,
        keyspace: &Keyspace,
        key: K,
        mut f: F,
    ) -> crate::Result<Option<UserValue>> {
        let key = key.into();
        let prev = self.get(keyspace, &key)?;
        let updated = f(prev.as_ref());

        if let Some(value) = updated {
            // NOTE: Skip insert if the value hasn't changed
            if prev.as_ref() != Some(&value) {
                self.insert(keyspace, key, value);
            }
        } else if prev.is_some() {
            self.remove(keyspace, key);
        }

        Ok(prev)
    }

    /// Retrieves an item from the transaction's state.
    ///
    /// The transaction allows reading your own writes (RYOW).
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub(super) fn get<K: AsRef<[u8]>>(
        &self,
        keyspace: &Keyspace,
        key: K,
    ) -> crate::Result<Option<UserValue>> {
        let key = key.as_ref();

        if let Some(memtable) = self.memtables.get(&keyspace.id) {
            if let Some(item) = memtable.get(key, SeqNo::MAX) {
                return Ok(ignore_tombstone_value(item).map(|x| x.value));
            }
        }

        let res = keyspace.tree.get(key, self.nonce.instant)?;

        Ok(res)
    }

    /// Retrieves the size of an item from the transaction's state.
    ///
    /// The transaction allows reading your own writes (RYOW).
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub(super) fn size_of<K: AsRef<[u8]>>(
        &self,
        keyspace: &Keyspace,
        key: K,
    ) -> crate::Result<Option<u32>> {
        let key = key.as_ref();

        if let Some(memtable) = self.memtables.get(&keyspace.id) {
            if let Some(item) = memtable.get(key, SeqNo::MAX) {
                // NOTE: Values are limited to u32 in lsm-tree
                #[allow(clippy::cast_possible_truncation)]
                return Ok(ignore_tombstone_value(item).map(|x| x.value.len() as u32));
            }
        }

        let res = keyspace.tree.size_of(key, self.nonce.instant)?;

        Ok(res)
    }

    /// Returns `true` if the transaction's state contains the specified key.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub(super) fn contains_key<K: AsRef<[u8]>>(
        &self,
        keyspace: &Keyspace,
        key: K,
    ) -> crate::Result<bool> {
        let key = key.as_ref();

        if let Some(memtable) = self.memtables.get(&keyspace.id) {
            if let Some(item) = memtable.get(key, SeqNo::MAX) {
                return Ok(!item.key.is_tombstone());
            }
        }

        let contains = keyspace.tree.contains_key(key, self.nonce.instant)?;

        Ok(contains)
    }

    /// Returns the first key-value pair in the transaction's state.
    /// The key in this pair is the minimum key in the transaction's state.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub(super) fn first_key_value(&self, keyspace: &Keyspace) -> crate::Result<Option<KvPair>> {
        self.iter(keyspace)
            .next()
            .map(Guard::into_inner)
            .transpose()
    }

    /// Returns the last key-value pair in the transaction's state.
    /// The key in this pair is the maximum key in the transaction's state.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub(super) fn last_key_value(&self, keyspace: &Keyspace) -> crate::Result<Option<KvPair>> {
        self.iter(keyspace)
            .next_back()
            .map(Guard::into_inner)
            .transpose()
    }

    /// Scans the entire keyspace, returning the amount of items.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub(super) fn len(&self, keyspace: &Keyspace) -> crate::Result<usize> {
        let mut count = 0;

        let iter = self.iter(keyspace);

        for guard in iter {
            let _ = guard.key()?;
            count += 1;
        }

        Ok(count)
    }

    /// Iterates over the transaction's state.
    ///
    /// Avoid using this function, or limit it as otherwise it may scan a lot of items.
    #[must_use]
    pub(super) fn iter<'a>(
        &'a self,
        keyspace: &'a Keyspace,
    ) -> impl DoubleEndedIterator<Item = Guard> + 'a {
        keyspace
            .tree
            .iter(
                self.nonce.instant,
                self.memtables.get(&keyspace.id).cloned(),
            )
            .map(Guard)
    }

    /// Iterates over a range of the transaction's state.
    ///
    /// Avoid using full or unbounded ranges as they may scan a lot of items (unless limited).
    #[must_use]
    pub(super) fn range<'a, K: AsRef<[u8]> + 'a, R: RangeBounds<K> + 'a>(
        &'a self,
        keyspace: &'a Keyspace,
        range: R,
    ) -> impl DoubleEndedIterator<Item = Guard> + 'a {
        keyspace
            .tree
            .range(
                range,
                self.nonce.instant,
                self.memtables.get(&keyspace.id).cloned(),
            )
            .map(Guard)
    }

    /// Iterates over a prefixed set of the transaction's state.
    ///
    /// Avoid using an empty prefix as it may scan a lot of items (unless limited).
    #[must_use]
    pub(super) fn prefix<'a, K: AsRef<[u8]> + 'a>(
        &'a self,
        keyspace: &'a Keyspace,
        prefix: K,
    ) -> impl DoubleEndedIterator<Item = Guard> + 'a {
        keyspace
            .tree
            .prefix(
                prefix,
                self.nonce.instant,
                self.memtables.get(&keyspace.id).cloned(),
            )
            .map(Guard)
    }

    /// Inserts a key-value pair into the keyspace.
    ///
    /// Keys may be up to 65536 bytes long, values up to 2^32 bytes.
    /// Shorter keys and values result in better performance.
    ///
    /// If the key already exists, the item will be overwritten.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub(super) fn insert<K: Into<UserKey>, V: Into<UserValue>>(
        &mut self,
        keyspace: &Keyspace,
        key: K,
        value: V,
    ) {
        // TODO: PERF: slow??
        self.memtables
            .entry(keyspace.id)
            .or_insert_with(|| Arc::new(Memtable::new(0)))
            .insert(lsm_tree::InternalValue::from_components(
                key,
                value,
                self.seqno,
                lsm_tree::ValueType::Value,
            ));

        self.seqno += 1;
    }

    /// Removes an item from the keyspace.
    ///
    /// The key may be up to 65536 bytes long.
    /// Shorter keys result in better performance.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub(super) fn remove<K: Into<UserKey>>(&mut self, keyspace: &Keyspace, key: K) {
        // TODO: PERF: slow??
        self.memtables
            .entry(keyspace.id)
            .or_insert_with(|| Arc::new(Memtable::new(0)))
            .insert(lsm_tree::InternalValue::new_tombstone(key, self.seqno));

        self.seqno += 1;
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
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub(super) fn remove_weak<K: Into<UserKey>>(&mut self, keyspace: &Keyspace, key: K) {
        // TODO: PERF: slow??
        self.memtables
            .entry(keyspace.id)
            .or_insert_with(|| Arc::new(Memtable::new(0)))
            .insert(lsm_tree::InternalValue::new_weak_tombstone(key, self.seqno));

        self.seqno += 1;
    }

    /// Commits the transaction.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub(super) fn commit(self) -> crate::Result<()> {
        // skip all the logic if no keys were written to
        if self.memtables.is_empty() {
            return Ok(());
        }

        let mut batch = WriteBatch::new(self.db).durability(self.durability);

        for (keyspace_id, memtable) in self.memtables {
            for item in memtable.iter() {
                batch.data.push(Item::new(
                    keyspace_id,
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

        Ok(())
    }

    /// More explicit alternative to dropping the transaction
    /// to roll it back.
    #[allow(clippy::unused_self)]
    pub(super) fn rollback(self) {}
}

#[cfg(test)]
mod tests {
    use crate::{
        tx::{single_writer::SingleWriterTxKeyspace, write_tx::BaseTransaction},
        KeyspaceCreateOptions, SingleWriterTxDatabase,
    };
    use tempfile::TempDir;

    struct TestEnv {
        db: SingleWriterTxDatabase,
        tree: SingleWriterTxKeyspace,

        #[allow(unused)]
        tmpdir: TempDir,
    }

    fn setup() -> Result<TestEnv, Box<dyn std::error::Error>> {
        let tmpdir: TempDir = tempfile::tempdir()?;
        let db = SingleWriterTxDatabase::builder(tmpdir.path()).open()?;

        let tree = db.keyspace("foo", KeyspaceCreateOptions::default())?;

        Ok(TestEnv { db, tree, tmpdir })
    }

    #[test]
    fn update_fetch() -> Result<(), Box<dyn std::error::Error>> {
        let env = setup()?;

        env.tree.insert([2u8], [20u8])?;

        let mut tx = BaseTransaction::new(
            env.db.inner.clone(),
            env.db.inner().supervisor.snapshot_tracker.open(),
        );

        let new = tx.update_fetch(env.tree.inner(), [2u8], |v| {
            v.and_then(|v| v.first().copied()).map(|v| [v + 5].into())
        })?;

        assert_eq!(new, Some([25u8].into()));
        tx.commit()?;

        Ok(())
    }
}
