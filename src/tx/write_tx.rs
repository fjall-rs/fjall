// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    batch::item::Item, keyspace::InternalKeyspaceId, snapshot_nonce::SnapshotNonce, Database,
    Guard, HashMap, Iter, Keyspace, PersistMode, Readable, WriteBatch,
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

impl Readable for BaseTransaction {
    fn get<K: AsRef<[u8]>>(
        &self,
        keyspace: impl AsRef<Keyspace>,
        key: K,
    ) -> crate::Result<Option<UserValue>> {
        let keyspace = keyspace.as_ref();
        let key = key.as_ref();

        if let Some(memtable) = self.memtables.get(&keyspace.id) {
            if let Some(item) = memtable.get(key, SeqNo::MAX) {
                return Ok(ignore_tombstone_value(item).map(|x| x.value));
            }
        }

        let res = keyspace.tree.get(key, self.nonce.instant)?;

        Ok(res)
    }

    fn contains_key<K: AsRef<[u8]>>(
        &self,
        keyspace: impl AsRef<Keyspace>,
        key: K,
    ) -> crate::Result<bool> {
        let keyspace = keyspace.as_ref();
        let key = key.as_ref();

        if let Some(memtable) = self.memtables.get(&keyspace.id) {
            if let Some(item) = memtable.get(key, SeqNo::MAX) {
                return Ok(!item.key.is_tombstone());
            }
        }

        let contains = keyspace.tree.contains_key(key, self.nonce.instant)?;

        Ok(contains)
    }

    fn first_key_value(&self, keyspace: impl AsRef<Keyspace>) -> crate::Result<Option<KvPair>> {
        self.iter(keyspace.as_ref())
            .next()
            .map(Guard::into_inner)
            .transpose()
    }

    fn last_key_value(&self, keyspace: impl AsRef<Keyspace>) -> crate::Result<Option<KvPair>> {
        self.iter(keyspace.as_ref())
            .next_back()
            .map(Guard::into_inner)
            .transpose()
    }

    fn size_of<K: AsRef<[u8]>>(
        &self,
        keyspace: impl AsRef<Keyspace>,
        key: K,
    ) -> crate::Result<Option<u32>> {
        let keyspace = keyspace.as_ref();
        let key = key.as_ref();

        if let Some(memtable) = self.memtables.get(&keyspace.id) {
            if let Some(item) = memtable.get(key, SeqNo::MAX) {
                // NOTE: Values are limited to u32 in lsm-tree
                #[expect(clippy::cast_possible_truncation)]
                return Ok(ignore_tombstone_value(item).map(|x| x.value.len() as u32));
            }
        }

        let res = keyspace.tree.size_of(key, self.nonce.instant)?;

        Ok(res)
    }

    fn iter(&self, keyspace: impl AsRef<Keyspace>) -> Iter {
        let keyspace = keyspace.as_ref();

        let iter = keyspace.tree.iter(
            self.nonce.instant,
            self.memtables
                .get(&keyspace.id)
                .cloned()
                .map(|mt| (mt, self.seqno)),
        );

        Iter::new(self.nonce.clone(), iter)
    }

    fn range<K: AsRef<[u8]>, R: RangeBounds<K>>(
        &self,
        keyspace: impl AsRef<Keyspace>,
        range: R,
    ) -> Iter {
        let keyspace = keyspace.as_ref();

        let iter = keyspace.tree.range(
            range,
            self.nonce.instant,
            self.memtables
                .get(&keyspace.id)
                .cloned()
                .map(|mt| (mt, self.seqno)),
        );

        Iter::new(self.nonce.clone(), iter)
    }

    fn prefix<K: AsRef<[u8]>>(&self, keyspace: impl AsRef<Keyspace>, prefix: K) -> Iter {
        let keyspace = keyspace.as_ref();

        let iter = keyspace.tree.prefix(
            prefix,
            self.nonce.instant,
            self.memtables
                .get(&keyspace.id)
                .cloned()
                .map(|mt| (mt, self.seqno)),
        );

        Iter::new(self.nonce.clone(), iter)
    }
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
    #[expect(clippy::unused_self)]
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

        #[expect(unused)]
        tmpdir: TempDir,
    }

    fn setup() -> Result<TestEnv, Box<dyn std::error::Error>> {
        let tmpdir: TempDir = tempfile::tempdir()?;
        let db = SingleWriterTxDatabase::builder(tmpdir.path()).open()?;

        let tree = db.keyspace("foo", KeyspaceCreateOptions::default)?;

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
