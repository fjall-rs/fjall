// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::conflict_manager::ConflictManager;
use crate::snapshot_tracker::SnapshotTracker;
use crate::SeqNo;
use std::collections::BTreeMap;
use std::ops::Bound::{Excluded, Unbounded};
use std::sync::{Mutex, MutexGuard};

#[derive(Default)]
pub struct CommittedTxnTracker(BTreeMap<SeqNo, ConflictManager>);

impl CommittedTxnTracker {
    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn insert(&mut self, seqno: SeqNo, cc: ConflictManager) {
        self.0.insert(seqno, cc);
    }

    pub fn gc(&mut self, safe_to_gc: SeqNo) {
        let Some((&first_to_keep, _)) = self.0.range((Excluded(safe_to_gc), Unbounded)).next()
        else {
            self.0.clear();
            return;
        };

        if self
            .0
            .first_key_value()
            .is_some_and(|(ts, _)| *ts == first_to_keep)
        {
            return;
        }

        self.0 = self.0.split_off(&first_to_keep);
    }

    pub fn has_conflict(&self, instant: SeqNo, cc: &ConflictManager) -> bool {
        self.0
            .range((instant + 1)..)
            .any(|(_ts, other_conflict_checker)| cc.has_conflict(other_conflict_checker))
    }
}

pub struct Oracle {
    pub(super) committed_txns: Mutex<CommittedTxnTracker>, // TODO: remove this lock, use Mutex<Oracle> instead
    pub(super) snapshot_tracker: SnapshotTracker,
}

impl Oracle {
    pub(crate) fn has_conflict(
        &self,
        instant: SeqNo,
        conflict_checker: &ConflictManager,
    ) -> crate::Result<bool> {
        let committed_txns = self
            .committed_txns
            .lock()
            .map_err(|_| crate::Error::Poisoned)?;

        Ok(committed_txns.has_conflict(instant, conflict_checker))
    }

    pub(crate) fn finalize(
        &self,
        seqno: SeqNo,
        conflict_checker: ConflictManager,
    ) -> crate::Result<()> {
        let mut committed_txns = self
            .committed_txns
            .lock()
            .map_err(|_| crate::Error::Poisoned)?;

        committed_txns.insert(seqno, conflict_checker);

        // TODO: This can be expensive and should probably be done in a background worker, or a after a memtable rotation
        // TODO: also only do this in write pipeline once?
        let safe_to_gc = self.snapshot_tracker.get_seqno_safe_to_gc();
        committed_txns.gc(safe_to_gc);

        Ok(())
    }

    pub(super) fn write_serialize_lock(
        &self,
    ) -> crate::Result<MutexGuard<'_, CommittedTxnTracker>> {
        self.committed_txns
            .lock()
            .map_err(|_| crate::Error::Poisoned)
    }
}

#[cfg(test)]
mod tests {
    use super::{CommittedTxnTracker, ConflictManager};
    use crate::{KeyspaceCreateOptions, OptimisticTxDatabase, OptimisticTxKeyspace, Readable};

    #[test]
    fn conflicts_gc_1() {
        let mut tracker = CommittedTxnTracker::default();
        tracker.insert(1, ConflictManager::default());
        tracker.insert(2, ConflictManager::default());
        tracker.insert(3, ConflictManager::default());

        tracker.gc(2);

        assert_eq!(1, tracker.len());
    }

    #[test]
    fn conflicts_gc_2() {
        let mut tracker = CommittedTxnTracker::default();
        tracker.insert(6, ConflictManager::default());
        tracker.insert(7, ConflictManager::default());
        tracker.insert(8, ConflictManager::default());

        tracker.gc(5);

        assert_eq!(3, tracker.len());
    }

    #[expect(clippy::significant_drop_tightening)]
    fn run_tx(
        db: &OptimisticTxDatabase,
        tree: &OptimisticTxKeyspace,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut tx1 = db.write_tx()?;
        let mut tx2 = db.write_tx()?;
        tx1.insert(tree.inner(), "hello", "world");

        tx1.commit()??;
        assert!(tree.contains_key("hello")?);

        _ = tx2.get(tree.inner(), "hello")?;

        tx2.insert(tree.inner(), "hello", "world2");
        assert!(tx2.commit()?.is_err()); // intended to conflict

        Ok(())
    }

    #[expect(clippy::unwrap_used)]
    #[test]
    fn oracle_committed_txns_does_not_leak() -> crate::Result<()> {
        let tmpdir = tempfile::tempdir()?;

        let db = OptimisticTxDatabase::builder(tmpdir.path()).open()?;
        let part = db.keyspace("foo", KeyspaceCreateOptions::default)?;

        for _ in 0..10_000 {
            run_tx(&db, &part).unwrap();
        }

        assert!(dbg!(db.oracle.committed_txns.lock().unwrap().len()) < 10_000);

        for _ in 0..10_000 {
            run_tx(&db, &part).unwrap();
        }

        assert!(dbg!(db.oracle.committed_txns.lock().unwrap().len()) < 10_000);

        Ok(())
    }

    #[test]
    fn committing_transaction_closes_only_its_snapshot() -> Result<(), Box<dyn std::error::Error>> {
        let tmpdir = tempfile::tempdir()?;

        let db = OptimisticTxDatabase::builder(tmpdir.path()).open()?;
        let tree = db.keyspace("foo", KeyspaceCreateOptions::default)?;

        let mut tx1 = db.write_tx()?;
        let tx2 = db.write_tx()?;
        assert_eq!(2, db.inner().supervisor.snapshot_tracker.open_snapshots());

        tx1.insert(tree.inner(), "hello", "world");
        tx1.commit()??;
        assert_eq!(1, db.inner().supervisor.snapshot_tracker.open_snapshots());

        drop(tx2);
        assert_eq!(0, db.inner().supervisor.snapshot_tracker.open_snapshots());

        Ok(())
    }
}
