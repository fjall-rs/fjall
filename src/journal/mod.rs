// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

pub mod batch_reader;
pub mod entry;
pub mod error;
pub mod manager;
pub mod reader;
mod recovery;
pub mod writer;

#[cfg(test)]
mod test;

use self::writer::PersistMode;
use crate::file::fsync_directory;
use batch_reader::JournalBatchReader;
use lsm_tree::CompressionType;
use reader::JournalReader;
use recovery::{recover_journals, RecoveryResult};
use std::{
    path::{Path, PathBuf},
    sync::{Mutex, MutexGuard},
};
use writer::Writer;

pub struct Journal {
    writer: Mutex<Writer>,
}

impl std::fmt::Debug for Journal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.path().display())
    }
}

impl Drop for Journal {
    fn drop(&mut self) {
        log::trace!("Dropping journal, trying to flush");

        match self.persist(PersistMode::SyncAll) {
            Ok(()) => {
                log::trace!("Flushed journal successfully");
            }
            Err(e) => {
                log::error!("Flush error on drop: {e:?}");
            }
        }

        #[cfg(feature = "__internal_whitebox")]
        crate::drop::decrement_drop_counter();
    }
}

impl Journal {
    pub fn with_compression(self, comp: CompressionType, threshold: usize) -> Self {
        {
            let mut writer = self.writer.lock().expect("lock is poisoned");
            writer.set_compression(comp, threshold);
        }
        self
    }

    fn from_file<P: AsRef<Path>>(path: P) -> crate::Result<Self> {
        Ok(Self {
            writer: Mutex::new(Writer::from_file(path)?),
        })
    }

    pub fn create_new<P: AsRef<Path>>(path: P) -> crate::Result<Self> {
        let path = path.as_ref();
        log::trace!("Creating new journal at {}", path.display());

        let folder = path.parent().expect("parent should exist");

        std::fs::create_dir_all(folder).inspect_err(|e| {
            log::error!(
                "Failed to create journal folder at {}: {e:?}",
                path.display(),
            );
        })?;

        let writer = Writer::create_new(path)?;

        // IMPORTANT: fsync folder on Unix
        fsync_directory(folder)?;

        #[cfg(feature = "__internal_whitebox")]
        crate::drop::increment_drop_counter();

        Ok(Self {
            writer: Mutex::new(writer),
        })
    }

    /// Hands out write access for the journal.
    pub(crate) fn get_writer(&self) -> MutexGuard<'_, Writer> {
        #[expect(clippy::expect_used)]
        self.writer.lock().expect("lock is poisoned")
    }

    pub fn path(&self) -> PathBuf {
        self.get_writer().path.clone()
    }

    pub fn get_reader(&self) -> crate::Result<JournalBatchReader> {
        let raw_reader = JournalReader::new(self.path())?;
        Ok(JournalBatchReader::new(raw_reader))
    }

    /// Persists the journal.
    pub fn persist(&self, mode: PersistMode) -> crate::Result<()> {
        let mut journal_writer = self.get_writer();
        journal_writer.persist(mode).map_err(Into::into)
    }

    pub fn recover<P: AsRef<Path>>(
        path: P,
        compression: CompressionType,
        compression_threshold: usize,
    ) -> crate::Result<RecoveryResult> {
        recover_journals(path, compression, compression_threshold)
    }
}
