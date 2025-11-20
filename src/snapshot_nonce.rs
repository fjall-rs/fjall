// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{snapshot_tracker::SnapshotTracker, SeqNo};

/// Holds a snapshot instant and automatically frees it from the snapshot tracker when dropped
pub struct SnapshotNonce {
    pub(crate) instant: SeqNo,
    tracker: SnapshotTracker,
}

impl std::fmt::Debug for SnapshotNonce {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SnapshotNonce({})", self.instant)
    }
}

impl SnapshotNonce {
    pub(crate) fn new(seqno: SeqNo, tracker: SnapshotTracker) -> Self {
        Self {
            instant: seqno,
            tracker,
        }
    }
}

impl Clone for SnapshotNonce {
    fn clone(&self) -> Self {
        self.tracker.clone_snapshot(self)
    }
}

impl Drop for SnapshotNonce {
    fn drop(&mut self) {
        self.tracker.close(self);
    }
}
