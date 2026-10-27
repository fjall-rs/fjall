// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{keyspace::InternalKeyspaceId, manifest::Manifest};

/// Writes version edits to the DB manifest
pub struct ManifestPersister {
    manifest: Manifest,
    keyspace_id: InternalKeyspaceId,
}

impl ManifestPersister {
    pub fn new(manifest: Manifest, keyspace_id: InternalKeyspaceId) -> Self {
        Self {
            manifest,
            keyspace_id,
        }
    }
}

impl lsm_tree::VersionEditPersister for ManifestPersister {
    fn persist(&self, edit: &lsm_tree::VersionEdit) -> lsm_tree::Result<()> {
        self.manifest
            .append(self.keyspace_id, edit)
            .expect("should not fail please fix"); // TODO: v4 handle error
        Ok(())
    }
}
