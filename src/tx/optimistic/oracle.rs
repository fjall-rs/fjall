// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::conflict_manager::ConflictManager;
use crate::snapshot_tracker::SnapshotTracker;
use crate::SeqNo;
use std::collections::BTreeMap;
use std::sync::{Mutex, MutexGuard};

pub enum CommitOutcome<E> {
    Ok,
    Aborted(E),
    Conflicted,
}

pub struct Oracle {
    pub(super) write_serialize_lock: Mutex<BTreeMap<u64, ConflictManager>>,
    pub(super) snapshot_tracker: SnapshotTracker,
}

impl Oracle {
    pub(super) fn with_commit<E, F: FnOnce() -> Result<(), E>>(
        &self,
        instant: SeqNo,
        conflict_checker: ConflictManager,
        f: F,
    ) -> crate::Result<CommitOutcome<E>> {
        let mut committed_txns = self
            .write_serialize_lock
            .lock()
            .map_err(|_| crate::Error::Poisoned)?;

        // If the committed_txn.ts is less than `SeqNo` that implies that the
        // committed_txn finished before the current transaction started.
        // We don't need to check for conflict in that case.
        // This change assumes linearizability. Lack of linearizability could
        // cause the read ts of a new txn to be lower than the commit ts of
        // a txn before it.
        let conflicted =
            committed_txns
                .range((instant + 1)..)
                .any(|(_ts, other_conflict_checker)| {
                    conflict_checker.has_conflict(other_conflict_checker)
                });

        self.snapshot_tracker.close_raw(instant);

        // TODO: This can be expensive and should probably be done in a background worker, or a after a memtable rotation
        let safe_to_gc = self.snapshot_tracker.get_seqno_safe_to_gc();
        committed_txns.retain(|ts, _| *ts > safe_to_gc);

        if conflicted {
            return Ok(CommitOutcome::Conflicted);
        }

        if let Err(e) = f() {
            return Ok(CommitOutcome::Aborted(e));
        }

        committed_txns.insert(self.snapshot_tracker.get(), conflict_checker);

        Ok(CommitOutcome::Ok)
    }

    pub(super) fn write_serialize_lock(
        &self,
    ) -> crate::Result<MutexGuard<'_, BTreeMap<u64, ConflictManager>>> {
        self.write_serialize_lock
            .lock()
            .map_err(|_| crate::Error::Poisoned)
    }
}

#[cfg(test)]
mod tests {
    use crate::{KeyspaceCreateOptions, OptimisticTxDatabase, OptimisticTxKeyspace, Readable};

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

        assert!(dbg!(db.oracle.write_serialize_lock.lock().unwrap().len()) < 10_000);

        for _ in 0..10_000 {
            run_tx(&db, &part).unwrap();
        }

        assert!(dbg!(db.oracle.write_serialize_lock.lock().unwrap().len()) < 10_000);

        Ok(())
    }
}
