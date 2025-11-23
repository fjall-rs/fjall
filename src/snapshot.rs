// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{snapshot_nonce::SnapshotNonce, Guard, Iter, Keyspace, Readable};
use lsm_tree::{AbstractTree, KvPair, SeqNo, UserValue};
use std::ops::RangeBounds;

/// A cross-keyspace snapshot
///
/// Snapshots keep a consistent view of the database at the time,
/// meaning old data will not be dropped until it is not referenced by any active transaction.
///
/// For that reason, you should try to keep transactions short-lived, and make sure they
/// are not held somewhere *forever*.
#[clippy::has_significant_drop]
pub struct Snapshot {
    pub(crate) nonce: SnapshotNonce,
}

impl Snapshot {
    pub(crate) fn new(nonce: SnapshotNonce) -> Self {
        Self { nonce }
    }

    #[doc(hidden)]
    #[must_use]
    pub fn seqno(&self) -> SeqNo {
        self.nonce.instant
    }
}

impl Readable for Snapshot {
    fn get<K: AsRef<[u8]>>(
        &self,
        keyspace: impl AsRef<Keyspace>,
        key: K,
    ) -> crate::Result<Option<UserValue>> {
        keyspace
            .as_ref()
            .tree
            .get(key, self.nonce.instant)
            .map_err(Into::into)
    }

    fn contains_key<K: AsRef<[u8]>>(
        &self,
        keyspace: impl AsRef<Keyspace>,
        key: K,
    ) -> crate::Result<bool> {
        keyspace
            .as_ref()
            .tree
            .contains_key(key, self.nonce.instant)
            .map_err(Into::into)
    }

    fn first_key_value(&self, keyspace: impl AsRef<Keyspace>) -> crate::Result<Option<KvPair>> {
        self.iter(keyspace)
            .next()
            .map(Guard::into_inner)
            .transpose()
    }

    fn last_key_value(&self, keyspace: impl AsRef<Keyspace>) -> crate::Result<Option<KvPair>> {
        self.iter(keyspace)
            .next_back()
            .map(Guard::into_inner)
            .transpose()
    }

    fn size_of<K: AsRef<[u8]>>(
        &self,
        keyspace: impl AsRef<Keyspace>,
        key: K,
    ) -> crate::Result<Option<u32>> {
        keyspace
            .as_ref()
            .tree
            .size_of(key, self.nonce.instant)
            .map_err(Into::into)
    }

    fn iter(&self, keyspace: impl AsRef<Keyspace>) -> Iter {
        let iter = keyspace.as_ref().tree.iter(self.nonce.instant, None);

        Iter::new(self.nonce.clone(), iter)
    }

    fn range<K: AsRef<[u8]>, R: RangeBounds<K>>(
        &self,
        keyspace: impl AsRef<Keyspace>,
        range: R,
    ) -> Iter {
        let iter = keyspace
            .as_ref()
            .tree
            .range(range, self.nonce.instant, None);

        Iter::new(self.nonce.clone(), iter)
    }

    fn prefix<K: AsRef<[u8]>>(&self, keyspace: impl AsRef<Keyspace>, prefix: K) -> Iter {
        let iter = keyspace
            .as_ref()
            .tree
            .prefix(prefix, self.nonce.instant, None);

        Iter::new(self.nonce.clone(), iter)
    }
}
