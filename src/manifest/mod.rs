// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

mod provider;

pub use provider::ManifestPersister;

use crate::keyspace::InternalKeyspaceId;
use hwal::{FragmentId, NameStrategy};
use std::{
    ffi::OsStr,
    fs::DirEntry,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

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

    pub fn create_new(path: &Path) -> crate::Result<Self> {
        log::trace!("Creating DB manifest at {}", path.display());

        Ok(Self {
            inner: Self::get_wal_builder().create_new(path)?,
        })
    }

    // TODO: manifest rotation + snapshot
    // TODO: we need to write the version, then sync to disk, then update current
    // TODO: then apply to hwal internal state...
    // TODO: e.g. wal.rotate_with(fn(file) -> Error)

    pub fn recover(path: &Path) -> crate::Result<Self> {
        log::trace!("Recovering DB manifest at {}", path.display());

        // TODO: read current, etc. etc.

        // TODO: we need a HWAL function to only recover the current version
        let mut recovery = Self::get_wal_builder().recover(path)?;

        for wal_record in recovery.by_ref() {
            let wal_record = wal_record?;

            log::warn!(
                "WAL record: {:?}",
                String::from_utf8_lossy(wal_record.payload()),
            );
        }

        Ok(Self {
            inner: recovery.into_writer()?,
        })
    }

    // TODO: append with a 0x1 byte, which says basically
    // "this VersionEdit" adjusts a keyspace-specific thing (keyspace ID + VersionEdit)
    pub fn append(
        &mut self,
        keyspace_id: InternalKeyspaceId,
        version_edit: &lsm_tree::VersionEdit,
    ) -> crate::Result<()> {
        log::trace!("Appending version edit {version_edit:#?} for keyspace #{keyspace_id}");

        // TODO: correctly encode version Edits and prefix with keyspace ID
        self.inner
            .append(format!("{keyspace_id:?} => {version_edit:?}").as_bytes())?;

        Ok(())
    }
}

#[derive(Clone)]
pub struct Manifest(Arc<Mutex<Inner>>);

impl Manifest {
    pub fn create_new(path: &Path) -> crate::Result<Self> {
        Inner::create_new(path).map(|inner| Self(Arc::new(Mutex::new(inner))))
    }

    pub fn recover(path: &Path) -> crate::Result<Self> {
        Inner::recover(path).map(|inner| Self(Arc::new(Mutex::new(inner))))
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
}
