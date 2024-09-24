use crate::snapshot_tracker::SnapshotTracker;
use crate::Instant;

use super::conflict_manager::ConflictChecker;
use lsm_tree::SequenceNumberCounter;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Mutex, PoisonError};

pub enum CommitOutcome<E> {
    Ok,
    Aborted(E),
    Conflicted,
}

pub struct Oracle {
    pub(super) write_serialize_lock: Mutex<BTreeMap<u64, ConflictChecker>>,
    pub(super) seqno: SequenceNumberCounter,
    pub(super) snapshot_tracker: SnapshotTracker,
}

impl Oracle {
    #[allow(clippy::nursery)]
    pub(super) fn with_commit<E, F: FnOnce() -> Result<(), E>>(
        &self,
        instant: Instant,
        conflict_checker: ConflictChecker,
        f: F,
    ) -> Result<CommitOutcome<E>, Error> {
        let mut committed_txns = self
            .write_serialize_lock
            .lock()
            .map_err(|_| Error::Poisoned)?;

        // If the committed_txn.ts is less than Instant that implies that the
        // committed_txn finished before the current transaction started.
        // We don't need to check for conflict in that case.
        // This change assumes linearizability. Lack of linearizability could
        // cause the read ts of a new txn to be lower than the commit ts of
        // a txn before it.
        for (_ts, other_conflict_checker) in committed_txns.range((instant + 1)..) {
            if conflict_checker.has_conflict(other_conflict_checker) {
                return Ok(CommitOutcome::Conflicted);
            }
        }

        self.snapshot_tracker.close(instant);
        let safe_to_gc = self.snapshot_tracker.get_seqno_safe_to_gc();

        committed_txns.retain(|ts, _| *ts > safe_to_gc);

        if let Err(e) = f() {
            return Ok(CommitOutcome::Aborted(e));
        }

        committed_txns.insert(self.seqno.get(), conflict_checker);

        Ok(CommitOutcome::Ok)
    }
}

#[derive(Debug)]
pub enum Error {
    /// Poisoned write serialization lock
    Poisoned,
}

impl std::error::Error for Error {}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl<T> From<PoisonError<T>> for Error {
    fn from(_value: PoisonError<T>) -> Self {
        Self::Poisoned
    }
}
