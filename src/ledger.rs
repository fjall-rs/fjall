// Copyright (c) 2026-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    keyspace::InternalKeyspaceId,
    manifest::{Manifest, ManifestPersister},
    wal::Wal,
};
use std::sync::MutexGuard;

/// The WAL and the DB manifest, behind one lock
///
/// Writes are appended to the WAL and applied to the active memtables under this lock,
/// and every manifest edit (flushes, compactions, keyspace changes) is persisted
/// and installed under it too.
///
/// So holding the lock means:
/// - no write can land in a memtable while it is sealed or flushed
/// - a manifest rotation can't miss an edit that was persisted, but not installed yet
pub struct Ledger {
    pub(crate) wal: Wal,
    pub(crate) manifest: Manifest,
}

/// Proof that the ledger lock is held
pub type LedgerGuard<'a> = MutexGuard<'a, Ledger>;

impl Ledger {
    pub fn new(wal: Wal, manifest: Manifest) -> Self {
        Self { wal, manifest }
    }

    /// Returns a persister that writes the version edits of the given keyspace to the manifest.
    pub fn persister(&mut self, keyspace_id: InternalKeyspaceId) -> ManifestPersister<'_> {
        ManifestPersister::new(&mut self.manifest, keyspace_id)
    }
}
