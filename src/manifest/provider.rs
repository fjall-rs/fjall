// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{keyspace::InternalKeyspaceId, manifest::Manifest};

/// Writes the version edits of one keyspace to the DB manifest
///
/// Borrows the manifest, so it can only be created while holding the ledger lock,
/// see [`crate::ledger::Ledger::persister`].
pub struct ManifestPersister<'a> {
    manifest: &'a mut Manifest,
    keyspace_id: InternalKeyspaceId,
}

impl<'a> ManifestPersister<'a> {
    pub fn new(manifest: &'a mut Manifest, keyspace_id: InternalKeyspaceId) -> Self {
        Self {
            manifest,
            keyspace_id,
        }
    }
}

impl lsm_tree::VersionEditPersister for ManifestPersister<'_> {
    type Error = crate::Error;

    fn persist(&mut self, edit: &lsm_tree::VersionEdit) -> crate::Result<()> {
        self.manifest.append(self.keyspace_id, edit)
    }
}
