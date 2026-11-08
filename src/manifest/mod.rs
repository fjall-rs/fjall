// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

mod coding;
mod db_edit;
mod provider;
mod recovery;

#[cfg(test)]
mod tests;

pub use db_edit::DbEditRecord;
pub use provider::ManifestPersister;

use crate::{
    file::rewrite_atomic,
    keyspace::InternalKeyspaceId,
    manifest::{
        coding::{encode_db_edit, encode_keyspace_version_edit},
        recovery::{recover_db_manifest, RecoveryResult},
    },
};
use hwal::{FragmentId, NameStrategy};
use std::{
    ffi::OsStr,
    fs::DirEntry,
    path::{Path, PathBuf},
    sync::Arc,
};

const CURRENT_VERSION_MARKER: &str = "current";
const PREALLOC_BYTES: u64 = 1_024 * 1_024; // 1 MiB

/// Manifest fragments are named `v{id}`
struct ManifestNameStrategy;

impl ManifestNameStrategy {
    /// Returns the fragment ID, if the file is a manifest fragment.
    ///
    /// NOTE: Only `v` + digits counts, so other files (e.g. `version`, `v0.bak`) are never
    /// mistaken for (and deleted as) manifest fragments
    fn parse_id(filename: &OsStr) -> Option<FragmentId> {
        let id = filename.to_str()?.strip_prefix('v')?;

        if id.is_empty() || !id.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }

        id.parse().ok()
    }
}

impl NameStrategy for ManifestNameStrategy {
    #[expect(clippy::expect_used)]
    fn extract_id(&self, filename: &OsStr) -> FragmentId {
        Self::parse_id(filename).expect("should be a manifest fragment")
    }

    fn fmt(&self, path: &Path, id: FragmentId) -> PathBuf {
        path.join(format!("v{id}"))
    }

    fn is_wal_file(&self, dirent: &DirEntry) -> bool {
        Self::parse_id(&dirent.file_name()).is_some()
    }
}

/// Reads the ID of the current manifest fragment from the `current` marker.
fn read_current_marker(path: &Path) -> crate::Result<FragmentId> {
    let marker_path = path.join(CURRENT_VERSION_MARKER);

    let bytes = std::fs::read(&marker_path).inspect_err(|e| {
        log::error!(
            "Could not read manifest marker at {}: {e:?}",
            marker_path.display(),
        );
    })?;

    let Ok(bytes) = <[u8; 8]>::try_from(bytes.as_slice()) else {
        log::error!(
            "Manifest marker at {} is corrupted ({} bytes)",
            marker_path.display(),
            bytes.len(),
        );
        return Err(crate::Error::Unrecoverable);
    };

    Ok(FragmentId::from_be_bytes(bytes))
}

/// Removes every manifest fragment except the current one.
///
/// Nothing is removed if the current fragment does not exist.
fn remove_stale_fragments(path: &Path, current_id: FragmentId) -> crate::Result<()> {
    // IMPORTANT: Only remove anything once the current fragment is known to exist,
    // otherwise a broken marker would delete the whole manifest
    let current_path = ManifestNameStrategy.fmt(path, current_id);

    if !current_path.try_exists()? {
        log::error!(
            "Current manifest fragment #{current_id} does not exist at {}",
            current_path.display(),
        );
        return Err(crate::Error::Unrecoverable);
    }

    let mut removed_any = false;

    for dirent in std::fs::read_dir(path)? {
        let dirent = dirent?;

        let Some(id) = ManifestNameStrategy::parse_id(&dirent.file_name()) else {
            continue;
        };

        if id == current_id {
            continue;
        }

        log::debug!("Removing stale manifest fragment #{id} (current is #{current_id})");
        std::fs::remove_file(dirent.path())?;
        removed_any = true;
    }

    if removed_any {
        crate::file::fsync_directory(path)?;
    }

    Ok(())
}

/// The DB manifest: keyspaces, their configuration and the version edits of their trees
///
/// IMPORTANT: Lives in the [`crate::ledger::Ledger`], next to the WAL, so it is only written
/// while holding the ledger lock.
pub struct Manifest {
    inner: hwal::Wal,
}

impl Manifest {
    fn get_wal_builder() -> hwal::Builder {
        hwal::Wal::builder()
            .with_preallocation_bytes(PREALLOC_BYTES)
            .with_name_strategy(Arc::new(ManifestNameStrategy))
    }

    pub(crate) fn create_new(path: &Path) -> crate::Result<Self> {
        log::trace!("Creating DB manifest at {}", path.display());

        Ok(Self {
            inner: Self::get_wal_builder().create_new(path)?,
        })
    }

    pub(crate) fn open(path: &Path) -> crate::Result<Self> {
        log::trace!("Opening DB manifest at {}", path.display());

        // IMPORTANT: Only the fragment named by the `current` marker counts: a manifest rotation
        // writes a full snapshot into the new fragment and syncs it, and only then updates the
        // marker.
        // So the current fragment is complete on its own, and every other fragment can
        // be removed before recovering:
        // - lower IDs are already contained in the snapshot
        // - higher IDs are from a rotation that crashed before updating the marker
        //
        // Then plain hwal recovery only sees the current fragment.
        let current_id = read_current_marker(path)?;
        remove_stale_fragments(path, current_id)?;

        Ok(Self {
            inner: Self::get_wal_builder().open(path)?,
        })
    }

    /// Writes a snapshot of the full DB state into a new manifest fragment, switches the `current`
    /// marker to it, and then removes the previous fragment.
    ///
    /// IMPORTANT: The snapshot must contain everything the previous fragment did
    /// (keyspaces, their configuration and versions, the legacy watermark).
    ///
    /// If this fails after rotating, new edits go to a fragment that `current` does not name,
    /// so the caller has to poison the database.
    pub(crate) fn rotate(
        &mut self,
        db_edits: &[DbEditRecord],
        keyspace_edits: impl Iterator<Item = (InternalKeyspaceId, lsm_tree::VersionEdit)>,
    ) -> crate::Result<()> {
        log::debug!(
            "Rotating DB manifest (active fragment #{})",
            self.inner.active_fragment_id(),
        );

        // 1. New fragment
        self.inner.rotate()?;

        // 2. Snapshot, synced before switching the marker
        if !db_edits.is_empty() {
            self.inner.append(&encode_db_edit(db_edits)?)?;
        }

        for (keyspace_id, version_edit) in keyspace_edits {
            self.inner
                .append(&encode_keyspace_version_edit(keyspace_id, &version_edit)?)?;
        }

        self.sync()?;

        // 3. Switch the marker (atomically)
        //
        // NOTE: A crash before this leaves the new fragment behind, which recovery removes
        self.place_current_marker()?;

        // 4. The previous fragment's contents are contained in the snapshot now
        //
        // NOTE: A crash before this leaves the previous fragment behind, which recovery removes
        self.inner.prune(|_| true)?;

        log::debug!(
            "Rotated DB manifest, active fragment now #{}",
            self.inner.active_fragment_id(),
        );

        Ok(())
    }

    pub(crate) fn append(
        &mut self,
        keyspace_id: InternalKeyspaceId,
        version_edit: &lsm_tree::VersionEdit,
    ) -> crate::Result<()> {
        log::trace!("Appending version edit {version_edit:#?} for keyspace #{keyspace_id}");
        self.inner
            .append(&encode_keyspace_version_edit(keyspace_id, version_edit)?)?;
        self.sync()
    }

    pub(crate) fn append_db_edits(&mut self, records: &[DbEditRecord]) -> crate::Result<()> {
        log::trace!("Appending DB edits {records:#?}");
        self.inner.append(&encode_db_edit(records)?)?;
        self.sync()
    }

    /// Makes appended edits durable before returning.
    ///
    /// IMPORTANT: hwal buffers appends in memory; without this, edits
    /// (keyspace creation, flushes, clears) only reach the file when the
    /// manifest is dropped, while the WAL may already reference them.
    fn sync(&mut self) -> crate::Result<()> {
        self.inner.flush(hwal::DurabilityLevel::SyncAll)?;
        Ok(())
    }

    pub(crate) fn place_current_marker(&self) -> crate::Result<()> {
        let id = self.inner.active_fragment_id();
        rewrite_atomic(
            &self.inner.path().join(CURRENT_VERSION_MARKER),
            &id.to_be_bytes(),
        )?;
        Ok(())
    }

    pub(crate) fn recover(&mut self) -> crate::Result<RecoveryResult> {
        recover_db_manifest(self)
    }

    /// Returns the size of the active manifest fragment in bytes.
    pub(crate) fn active_fragment_size(&self) -> u64 {
        self.inner.active_fragment_size()
    }

    /// Returns the size of the manifest in bytes.
    pub(crate) fn disk_usage(&self) -> crate::Result<u64> {
        self.inner.disk_usage().map_err(Into::into)
    }
}
