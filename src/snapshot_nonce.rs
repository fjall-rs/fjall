// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{snapshot_tracker::SnapshotTracker, SeqNo};

/// Holds a snapshot instant and automatically frees it from the snapshot tracker when dropped
pub struct SnapshotNonce {
    pub(crate) instant: SeqNo,
    tracker: SnapshotTracker,
}

impl Clone for SnapshotNonce {
    fn clone(&self) -> Self {
        // IMPORTANT: Increment snapshot count in tracker
        self.tracker.open(self.instant);

        Self {
            instant: self.instant,
            tracker: self.tracker.clone(),
        }
    }
}

impl Drop for SnapshotNonce {
    fn drop(&mut self) {
        self.tracker.close(self.instant);
    }
}

impl SnapshotNonce {
    pub fn new(instant: SeqNo, tracker: SnapshotTracker) -> Self {
        tracker.open(instant);
        Self { instant, tracker }
    }
}
