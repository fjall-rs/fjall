use std::{
    fmt,
    ops::{Bound, RangeBounds, RangeFull},
};

use lsm_tree::{KvPair, Slice, UserKey, UserValue};

use crate::{
    snapshot_nonce::SnapshotNonce,
    tx::{
        conflict_manager::ConflictManager,
        oracle::{self, CreateCommitTimestampResult},
    },
    PersistMode, TxKeyspace, TxPartitionHandle,
};

use super::WriteTransaction as InnerWriteTransaction;

#[derive(Debug)]
pub enum Error {
    /// Oracle-related error
    Oracle(oracle::Error),
}

impl From<oracle::Error> for Error {
    fn from(value: oracle::Error) -> Self {
        Self::Oracle(value)
    }
}

impl std::error::Error for Error {}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

#[derive(Debug)]
pub struct Conflict;

impl std::error::Error for Conflict {}

impl fmt::Display for Conflict {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        "transaction conflict".fmt(f)
    }
}

pub struct WriteTransaction {
    inner: InnerWriteTransaction,
    cm: ConflictManager,
    read_ts: u64,
    done_read: bool,
}

impl WriteTransaction {
    pub(crate) fn new(keyspace: TxKeyspace, nonce: SnapshotNonce, read_ts: u64) -> Self {
        Self {
            inner: InnerWriteTransaction::new(keyspace, nonce),
            cm: ConflictManager::default(),
            read_ts,
            done_read: false,
        }
    }

    /// Sets the durability level.
    #[must_use]
    pub fn durability(mut self, mode: Option<PersistMode>) -> Self {
        self.inner = self.inner.durability(mode);
        self
    }

    pub fn take<K: AsRef<[u8]>>(
        &mut self,
        partition: &TxPartitionHandle,
        key: K,
    ) -> crate::Result<Option<UserValue>> {
        self.fetch_update(partition, key, |_| None)
    }

    pub fn update_fetch<K: AsRef<[u8]>, F: Fn(Option<&UserValue>) -> Option<UserValue>>(
        &mut self,
        partition: &TxPartitionHandle,
        key: K,
        f: F,
    ) -> crate::Result<Option<UserValue>> {
        let updated = self.inner.update_fetch(partition, key.as_ref(), f)?;

        self.cm
            .mark_read(&partition.inner.name, &key.as_ref().into());
        self.cm
            .mark_conflict(&partition.inner.name, &key.as_ref().into());

        Ok(updated)
    }

    pub fn fetch_update<K: AsRef<[u8]>, F: Fn(Option<&UserValue>) -> Option<UserValue>>(
        &mut self,
        partition: &TxPartitionHandle,
        key: K,
        f: F,
    ) -> crate::Result<Option<UserValue>> {
        let prev = self.inner.fetch_update(partition, key.as_ref(), f)?;

        self.cm
            .mark_read(&partition.inner.name, &key.as_ref().into());
        self.cm
            .mark_conflict(&partition.inner.name, &key.as_ref().into());

        Ok(prev)
    }

    pub fn get<K: AsRef<[u8]>>(
        &self,
        partition: &TxPartitionHandle,
        key: K,
    ) -> crate::Result<Option<UserValue>> {
        let res = self.inner.get(partition, key.as_ref())?;

        self.cm
            .mark_read(&partition.inner.name, &key.as_ref().into());

        Ok(res)
    }

    pub fn contains_key<K: AsRef<[u8]>>(
        &self,
        partition: &TxPartitionHandle,
        key: K,
    ) -> crate::Result<bool> {
        let contains = self.inner.contains_key(partition, key.as_ref())?;

        self.cm
            .mark_read(&partition.inner.name, &key.as_ref().into());

        Ok(contains)
    }

    pub fn first_key_value(&self, partition: &TxPartitionHandle) -> crate::Result<Option<KvPair>> {
        // TODO: calling .iter will mark the partition as fully read, is that what we want?
        self.iter(partition).next().transpose()
    }

    pub fn last_key_value(&self, partition: &TxPartitionHandle) -> crate::Result<Option<KvPair>> {
        // TODO: calling .iter will mark the partition as fully read, is that what we want?
        self.iter(partition).next_back().transpose()
    }

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

    #[must_use]
    pub fn iter(
        &self,
        partition: &TxPartitionHandle,
    ) -> impl DoubleEndedIterator<Item = crate::Result<KvPair>> + 'static {
        self.cm.mark_range(&partition.inner.name, RangeFull);

        self.inner.iter(partition)
    }

    #[must_use]
    pub fn keys(
        &self,
        partition: &TxPartitionHandle,
    ) -> impl DoubleEndedIterator<Item = crate::Result<UserKey>> + 'static {
        self.cm.mark_range(&partition.inner.name, RangeFull);

        self.inner.keys(partition)
    }

    #[must_use]
    pub fn values(
        &self,
        partition: &TxPartitionHandle,
    ) -> impl DoubleEndedIterator<Item = crate::Result<UserValue>> + 'static {
        self.cm.mark_range(&partition.inner.name, RangeFull);

        self.inner.values(partition)
    }

    #[must_use]
    pub fn range<'b, K: AsRef<[u8]> + 'b, R: RangeBounds<K> + 'b>(
        &'b self,
        partition: &'b TxPartitionHandle,
        range: R,
    ) -> impl DoubleEndedIterator<Item = crate::Result<KvPair>> + 'static {
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
        self.cm.mark_range(&partition.inner.name, (start, end));

        self.inner.range(partition, range)
    }

    #[must_use]
    pub fn prefix<'b, K: AsRef<[u8]> + 'b>(
        &'b self,
        partition: &'b TxPartitionHandle,
        prefix: K,
    ) -> impl DoubleEndedIterator<Item = crate::Result<KvPair>> + 'static {
        self.cm.mark_range(
            &partition.inner.name,
            lsm_tree::range::prefix_to_range(prefix.as_ref()),
        );

        self.inner.prefix(partition, prefix)
    }

    pub fn insert<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        &mut self,
        partition: &TxPartitionHandle,
        key: K,
        value: V,
    ) {
        self.inner.insert(partition, key.as_ref(), value);

        self.cm
            .mark_conflict(&partition.inner.name, &key.as_ref().into());
    }

    pub fn remove<K: AsRef<[u8]>>(&mut self, partition: &TxPartitionHandle, key: K) {
        self.inner.remove(partition, key.as_ref());

        self.cm
            .mark_conflict(&partition.inner.name, &key.as_ref().into());
    }

    pub fn commit(mut self) -> crate::Result<Result<(), Conflict>> {
        // skip all the logic if no keys were written to
        if self.inner.memtables.is_empty() {
            return Ok(Ok(()));
        }

        let orc = self.inner.keyspace.orc.clone();
        let _write_lock = orc
            .write_serialize_lock
            .lock()
            .map_err(oracle::Error::from)
            .map_err(Error::from)?;

        let cm_res = self.inner.keyspace.orc.new_commit_ts(
            &mut self.done_read,
            self.read_ts,
            self.cm.into(),
        );

        let commit_ts = match cm_res {
            Err(e) => {
                return Err(crate::Error::Ssi(e.into()));
            }
            Ok(CreateCommitTimestampResult::Timestamp(ts)) => ts,
            Ok(CreateCommitTimestampResult::Conflict(_conflict_manager)) => {
                return Ok(Err(Conflict));
            }
        };

        self.inner.commit()?;

        orc.done_commit(commit_ts).map_err(Error::from)?;

        Ok(Ok(()))
    }

    pub fn rollback(self) {
        self.inner.rollback();
    }
}

#[cfg(test)]
mod tests {
    use crate::{tx::write::ssi::Conflict, Config, PartitionCreateOptions};
    use test_log::test;

    #[test]
    fn basic_tx_test() -> Result<(), Box<dyn std::error::Error>> {
        let tmpdir = tempfile::tempdir()?;
        let keyspace = Config::new(tmpdir.path()).open_transactional()?;

        let part = keyspace.open_partition("foo", PartitionCreateOptions::default())?;

        let mut tx1 = keyspace.write_tx()?;
        let mut tx2 = keyspace.write_tx()?;

        tx1.insert(&part, "hello", "world");

        tx1.commit()??;
        assert!(part.contains_key("hello")?);

        assert_eq!(tx2.get(&part, "hello")?, None);

        tx2.insert(&part, "hello", "world2");
        assert!(matches!(tx2.commit()?, Err(Conflict)));

        let mut tx1 = keyspace.write_tx()?;
        let mut tx2 = keyspace.write_tx()?;

        tx1.iter(&part).next();
        tx2.insert(&part, "hello", "world2");

        tx1.insert(&part, "hello2", "world1");
        tx1.commit()??;

        tx2.commit()??;

        Ok(())
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn tx_ssi_swap() -> Result<(), Box<dyn std::error::Error>> {
        let tmpdir = tempfile::tempdir()?;
        let keyspace = Config::new(tmpdir.path()).open_transactional()?;

        let part = keyspace.open_partition("foo", PartitionCreateOptions::default())?;

        part.insert("x", "x")?;
        part.insert("y", "y")?;

        let mut tx1 = keyspace.write_tx()?;
        let mut tx2 = keyspace.write_tx()?;

        {
            let x = tx1.get(&part, "x")?.unwrap();
            tx1.insert(&part, "y", x);
        }

        {
            let y = tx2.get(&part, "y")?.unwrap();
            tx2.insert(&part, "x", y);
        }

        tx1.commit()??;
        assert!(matches!(tx2.commit()?, Err(Conflict)));

        Ok(())
    }
}
