use crate::{snapshot_tracker::SnapshotTracker, Instant};

/// Holds a snapshot instant and automatically frees it from the snapshot tracker when dropped
pub struct SnapshotNonce {
    instant: Instant,
    tracker: SnapshotTracker,
}

impl Drop for SnapshotNonce {
    fn drop(&mut self) {
        self.tracker.close(self.instant);
    }
}

impl SnapshotNonce {
    pub fn new(instant: Instant, tracker: SnapshotTracker) -> Self {
        tracker.open(instant);
        Self { instant, tracker }
    }
}
