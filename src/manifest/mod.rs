// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

mod coding;
mod db_edit;
mod provider;
mod recovery;

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
    sync::{Arc, Mutex},
};

const CURRENT_VERSION_MARKER: &str = "current";
const PREALLOC_BYTES: u64 = 1_024 * 1_024; // 1 MiB

struct ManifestNameStrategy;

impl NameStrategy for ManifestNameStrategy {
    #[warn(clippy::unwrap_used)]
    fn extract_id(&self, filename: &OsStr) -> FragmentId {
        let s = filename.to_str().unwrap();
        let s = s.strip_prefix("v").unwrap();
        s.parse().unwrap()
    }

    fn fmt(&self, path: &Path, id: FragmentId) -> PathBuf {
        path.join(format!("v{id}"))
    }

    fn is_wal_file(&self, dirent: &DirEntry) -> bool {
        let f = dirent.file_name();
        let f = f.as_encoded_bytes();
        f.starts_with(b"v") && f != b"version"
    }
}

struct Inner {
    inner: hwal::Wal,
}

impl Inner {
    fn get_wal_builder() -> hwal::Builder {
        hwal::Wal::builder()
            .with_preallocation_bytes(PREALLOC_BYTES)
            .with_name_strategy(Arc::new(ManifestNameStrategy))
    }

    fn create_new(path: &Path) -> crate::Result<Self> {
        log::trace!("Creating DB manifest at {}", path.display());

        Ok(Self {
            inner: Self::get_wal_builder().create_new(path)?,
        })
    }

    fn open(path: &Path) -> crate::Result<Self> {
        log::trace!("Opening DB manifest at {}", path.display());

        Ok(Self {
            inner: Self::get_wal_builder().open(path)?,
        })
    }

    // TODO: manifest rotation + snapshot
    // TODO: we need to write the version, then sync to disk, then update current
    // TODO: then apply to hwal internal state...
    // TODO: e.g. wal.rotate_with(fn(file) -> Error)

    fn append(
        &mut self,
        keyspace_id: InternalKeyspaceId,
        version_edit: &lsm_tree::VersionEdit,
    ) -> crate::Result<()> {
        log::trace!("Appending version edit {version_edit:#?} for keyspace #{keyspace_id}");
        self.inner
            .append(&encode_keyspace_version_edit(keyspace_id, version_edit)?)?;
        self.sync()
    }

    fn append_db_edit(&mut self, records: &[DbEditRecord]) -> crate::Result<()> {
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

    fn place_current_marker(&self) -> crate::Result<()> {
        let id = self.inner.active_fragment_id();
        rewrite_atomic(&self.inner.path().join("current"), &id.to_be_bytes())?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct Manifest(Arc<Mutex<Inner>>);

impl Manifest {
    pub fn create_new(path: &Path) -> crate::Result<Self> {
        Inner::create_new(path).map(|inner| Self(Arc::new(Mutex::new(inner))))
    }

    pub fn open(path: &Path) -> crate::Result<Self> {
        Inner::open(path).map(|inner| Self(Arc::new(Mutex::new(inner))))
    }

    pub fn recover(&self) -> crate::Result<RecoveryResult> {
        let mut inner = self.0.lock().expect("lock should not be poisoned");
        recover_db_manifest(&mut inner)
    }

    pub fn append(
        &self,
        keyspace_id: InternalKeyspaceId,
        version_edit: &lsm_tree::VersionEdit,
    ) -> crate::Result<()> {
        self.0
            .lock()
            .expect("should not be poisoned")
            .append(keyspace_id, version_edit)
    }

    pub fn append_db_edits(&self, records: &[DbEditRecord]) -> crate::Result<()> {
        self.0
            .lock()
            .expect("should not be poisoned")
            .append_db_edit(records)
    }

    pub fn place_current_marker(&self) -> crate::Result<()> {
        self.0
            .lock()
            .expect("should not be poisoned")
            .place_current_marker()
    }
}
