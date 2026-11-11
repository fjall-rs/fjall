// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{snapshot_tracker::Shard, SeqNo};

/// Holds a snapshot instant and automatically frees it from the snapshot tracker when dropped
pub struct SnapshotNonce {
    pub(crate) instant: SeqNo,

    /// The tracker shard the snapshot is registered in
    ///
    /// NOTE: Only the shard, not the whole tracker: cloning and dropping a nonce then only
    /// touches the shard's reference count, which is shared with far fewer threads.
    /// A snapshot may also be cloned or dropped on another thread than the one that opened it.
    shard: Shard,
}

impl std::fmt::Debug for SnapshotNonce {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SnapshotNonce({})", self.instant)
    }
}

impl SnapshotNonce {
    /// IMPORTANT: The snapshot must already be registered in `shard`
    pub(crate) fn new(seqno: SeqNo, shard: Shard) -> Self {
        Self {
            instant: seqno,
            shard,
        }
    }
}

impl Clone for SnapshotNonce {
    fn clone(&self) -> Self {
        #[expect(clippy::expect_used)]
        self.shard
            .lock()
            .expect("should not be poisoned")
            .entry(self.instant)
            .and_modify(|v| *v += 1)
            .or_insert_with(|| unreachable!("cloning snapshot should already have count > 0"));

        Self::new(self.instant, self.shard.clone())
    }
}

impl Drop for SnapshotNonce {
    fn drop(&mut self) {
        #[expect(clippy::expect_used)]
        let mut shard = self.shard.lock().expect("should not be poisoned");

        // NOTE: Every snapshot is closed exactly once, here
        let Some(v) = shard.get_mut(&self.instant) else {
            unreachable!("closing a snapshot should already have count > 0");
        };

        *v -= 1;

        // NOTE: Closed snapshots must not hold back the GC watermark
        // (`get_seqno_safe_to_gc` takes the lowest key of each shard)
        if *v == 0 {
            shard.remove(&self.instant);
        }
    }
}
