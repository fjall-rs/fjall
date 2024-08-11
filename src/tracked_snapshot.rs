// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::snapshot_nonce::SnapshotNonce;

/// A snapshot captures a read-only point-in-time view of the tree at the time the snapshot was created
///
/// As long as the snapshot is open, old versions of objects will not be evicted as to
/// keep the snapshot consistent. Thus, snapshots should only be kept around for as little as possible.
///
/// Snapshots do not persist across restarts.
pub struct TrackedSnapshot {
    inner: lsm_tree::Snapshot,

    #[allow(unused)]
    nonce: SnapshotNonce,
}

impl std::ops::Deref for TrackedSnapshot {
    type Target = lsm_tree::Snapshot;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl TrackedSnapshot {
    pub(crate) fn new(snapshot: lsm_tree::Snapshot, nonce: SnapshotNonce) -> Self {
        Self {
            inner: snapshot,
            nonce,
        }
    }
}
