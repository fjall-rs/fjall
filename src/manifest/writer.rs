// Copyright (c) 2026-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::coding::{encode_db_edit, encode_keyspace_version_edit};
use crate::{keyspace::InternalKeyspaceId, manifest::DbEditRecord};

/// Receives the records of a manifest snapshot, see [`crate::supervisor::Supervisor::write_manifest_snapshot`]
pub trait ManifestWriter {
    /// Writes DB edits (keyspaces, their configuration, ...) as one record.
    fn append_db_edits(&mut self, records: &[DbEditRecord]) -> crate::Result<()>;

    /// Writes the version edit of a keyspace's tree.
    fn append_version_edit(
        &mut self,
        keyspace_id: InternalKeyspaceId,
        version_edit: &lsm_tree::VersionEdit,
    ) -> crate::Result<()>;
}

/// Appends snapshot records to the active manifest fragment, without syncing
/// (see [`super::Manifest::rotate`], which syncs once at the end)
pub(super) struct FragmentWriter<'a>(pub(super) &'a mut hwal::Wal);

impl ManifestWriter for FragmentWriter<'_> {
    fn append_db_edits(&mut self, records: &[DbEditRecord]) -> crate::Result<()> {
        if records.is_empty() {
            return Ok(());
        }

        self.0.append(&encode_db_edit(records)?)?;
        Ok(())
    }

    fn append_version_edit(
        &mut self,
        keyspace_id: InternalKeyspaceId,
        version_edit: &lsm_tree::VersionEdit,
    ) -> crate::Result<()> {
        self.0
            .append(&encode_keyspace_version_edit(keyspace_id, version_edit)?)?;
        Ok(())
    }
}
