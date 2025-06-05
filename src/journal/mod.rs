// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

pub mod batch_reader;
pub mod error;
pub mod manager;
pub mod marker;
pub mod reader;
mod recovery;
pub mod writer;

use self::writer::PersistMode;
use crate::file::fsync_directory;
use batch_reader::JournalBatchReader;
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
        write!(f, "{:?}", self.path())
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
    fn from_file<P: AsRef<Path>>(path: P) -> crate::Result<Self> {
        Ok(Self {
            writer: Mutex::new(Writer::from_file(path)?),
        })
    }

    pub fn create_new<P: AsRef<Path>>(path: P) -> crate::Result<Self> {
        let path = path.as_ref();
        log::trace!("Creating new journal at {path:?}");

        let folder = path.parent().expect("parent should exist");
        std::fs::create_dir_all(folder).inspect_err(|e| {
            log::error!("Failed to create journal folder at {path:?}: {e:?}");
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
        let mut lock = self.get_writer();
        lock.persist(mode).map_err(Into::into)
    }

    pub fn recover<P: AsRef<Path>>(path: P) -> crate::Result<RecoveryResult> {
        recover_journals(path)
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::batch::item::Item as BatchItem;
    use lsm_tree::{coding::Encode, ValueType};
    use marker::Marker;
    use std::io::Write;
    use tempfile::tempdir;
    use test_log::test;

    #[test]
    fn journal_rotation() -> crate::Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("0");
        let next_path = dir.path().join("1");

        {
            let journal = Journal::create_new(&path)?;
            let mut writer = journal.get_writer();

            writer.write_batch(
                [
                    BatchItem::new("default", *b"a", *b"a", ValueType::Value),
                    BatchItem::new("default", *b"b", *b"b", ValueType::Value),
                ]
                .iter(),
                2,
                0,
            )?;
            writer.rotate()?;
        }

        assert!(path.try_exists()?);
        assert!(next_path.try_exists()?);

        Ok(())
    }

    #[test]
    fn journal_recovery_active() -> crate::Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("0");
        let next_path = dir.path().join("1");
        let next_next_path = dir.path().join("2");

        {
            let journal = Journal::create_new(&path)?;
            let mut writer = journal.get_writer();

            writer.write_batch(
                [
                    BatchItem::new("default", *b"a", *b"a", ValueType::Value),
                    BatchItem::new("default", *b"b", *b"b", ValueType::Value),
                ]
                .iter(),
                2,
                0,
            )?;
            writer.rotate()?;

            writer.write_batch(
                [
                    BatchItem::new("default2", *b"c", *b"c", ValueType::Value),
                    BatchItem::new("default2", *b"d", *b"d", ValueType::Value),
                ]
                .iter(),
                2,
                1,
            )?;
            writer.rotate()?;

            writer.write_batch(
                [
                    BatchItem::new("default3", *b"c", *b"c", ValueType::Value),
                    BatchItem::new("default3", *b"d", *b"d", ValueType::Value),
                ]
                .iter(),
                2,
                1,
            )?;
        }

        assert!(path.try_exists()?);
        assert!(next_path.try_exists()?);
        assert!(next_next_path.try_exists()?);

        let journal_recovered = Journal::recover(dir)?;
        assert_eq!(journal_recovered.active.path(), next_next_path);
        assert_eq!(journal_recovered.sealed, &[(0, path), (1, next_path)]);

        Ok(())
    }

    #[test]
    fn journal_recovery_no_active() -> crate::Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("0");
        let next_path = dir.path().join("1");

        {
            let journal = Journal::create_new(&path)?;

            {
                let mut writer = journal.get_writer();

                writer.write_batch(
                    [
                        BatchItem::new("default", *b"a", *b"a", ValueType::Value),
                        BatchItem::new("default", *b"b", *b"b", ValueType::Value),
                    ]
                    .iter(),
                    2,
                    0,
                )?;
                writer.rotate()?;
            }

            // NOTE: Delete the new, active journal -> old journal will be
            // reused as active on next recovery
            std::fs::remove_file(&next_path)?;
        }

        assert!(path.try_exists()?);
        assert!(!next_path.try_exists()?);

        let journal_recovered = Journal::recover(dir)?;
        assert_eq!(journal_recovered.active.path(), path);
        assert_eq!(journal_recovered.sealed, &[]);

        Ok(())
    }

    #[test]
    fn journal_truncation_corrupt_bytes() -> crate::Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("0");

        let values = [
            BatchItem::new("default", *b"abc", *b"def", ValueType::Value),
            BatchItem::new("default", *b"yxc", *b"ghj", ValueType::Value),
        ];

        {
            let journal = Journal::create_new(&path)?;
            journal
                .get_writer()
                .write_batch(values.iter(), values.len(), 0)?;
        }

        {
            let journal = Journal::from_file(&path)?;
            let reader = journal.get_reader()?;
            let collected = reader.flatten().collect::<Vec<_>>();
            assert_eq!(values.to_vec(), collected.first().unwrap().items);
        }

        // Mangle journal
        {
            let mut file = std::fs::OpenOptions::new().append(true).open(&path)?;
            file.write_all(b"09pmu35w3a9mp53bao9upw3ab5up")?;
            file.sync_all()?;
        }

        for _ in 0..10 {
            let journal = Journal::from_file(&path)?;
            let reader = journal.get_reader()?;
            let collected = reader.flatten().collect::<Vec<_>>();
            assert_eq!(values.to_vec(), collected.first().unwrap().items);
        }

        // Mangle journal
        for _ in 0..5 {
            let mut file = std::fs::OpenOptions::new().append(true).open(&path)?;
            file.write_all(b"09pmu35w3a9mp53bao9upw3ab5up")?;
            file.sync_all()?;
        }

        for _ in 0..10 {
            let journal = Journal::from_file(&path)?;
            let reader = journal.get_reader()?;
            let collected = reader.flatten().collect::<Vec<_>>();
            assert_eq!(values.to_vec(), collected.first().unwrap().items);
        }

        Ok(())
    }

    #[test]
    fn journal_truncation_repeating_start_marker() -> crate::Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("0");

        let values = [
            BatchItem::new("default", *b"abc", *b"def", ValueType::Value),
            BatchItem::new("default", *b"yxc", *b"ghj", ValueType::Value),
        ];

        {
            let journal = Journal::create_new(&path)?;
            journal
                .get_writer()
                .write_batch(values.iter(), values.len(), 0)?;
        }

        {
            let journal = Journal::from_file(&path)?;
            let reader = journal.get_reader()?;
            let collected = reader.flatten().collect::<Vec<_>>();
            assert_eq!(values.to_vec(), collected.first().unwrap().items);
        }

        // Mangle journal
        {
            let mut file = std::fs::OpenOptions::new().append(true).open(&path)?;
            Marker::Start {
                item_count: 2,
                seqno: 64,
                compression: lsm_tree::CompressionType::None,
            }
            .encode_into(&mut file)?;
            file.sync_all()?;
        }

        for _ in 0..10 {
            let journal = Journal::from_file(&path)?;
            let reader = journal.get_reader()?;
            let collected = reader.flatten().collect::<Vec<_>>();
            assert_eq!(values.to_vec(), collected.first().unwrap().items);
        }

        // Mangle journal
        for _ in 0..5 {
            let mut file = std::fs::OpenOptions::new().append(true).open(&path)?;
            Marker::Start {
                item_count: 2,
                seqno: 64,
                compression: lsm_tree::CompressionType::None,
            }
            .encode_into(&mut file)?;
            file.sync_all()?;
        }

        for _ in 0..10 {
            let journal = Journal::from_file(&path)?;
            let reader = journal.get_reader()?;
            let collected = reader.flatten().collect::<Vec<_>>();
            assert_eq!(values.to_vec(), collected.first().unwrap().items);
        }

        Ok(())
    }

    #[test]
    fn journal_truncation_repeating_end_marker() -> crate::Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("0");

        let values = [
            BatchItem::new("default", *b"abc", *b"def", ValueType::Value),
            BatchItem::new("default", *b"yxc", *b"ghj", ValueType::Value),
        ];

        {
            let journal = Journal::create_new(&path)?;
            journal
                .get_writer()
                .write_batch(values.iter(), values.len(), 0)?;
        }

        {
            let journal = Journal::from_file(&path)?;
            let reader = journal.get_reader()?;
            let collected = reader.flatten().collect::<Vec<_>>();
            assert_eq!(values.to_vec(), collected.first().unwrap().items);
        }

        // Mangle journal
        {
            let mut file = std::fs::OpenOptions::new().append(true).open(&path)?;
            Marker::End(5432).encode_into(&mut file)?;
            file.sync_all()?;
        }

        for _ in 0..10 {
            let journal = Journal::from_file(&path)?;
            let reader = journal.get_reader()?;
            let collected = reader.flatten().collect::<Vec<_>>();
            assert_eq!(values.to_vec(), collected.first().unwrap().items);
        }

        // Mangle journal
        for _ in 0..5 {
            let mut file = std::fs::OpenOptions::new().append(true).open(&path)?;
            Marker::End(5432).encode_into(&mut file)?;
            file.sync_all()?;
        }

        for _ in 0..10 {
            let journal = Journal::from_file(&path)?;
            let reader = journal.get_reader()?;
            let collected = reader.flatten().collect::<Vec<_>>();
            assert_eq!(values.to_vec(), collected.first().unwrap().items);
        }

        Ok(())
    }

    #[test]
    fn journal_truncation_repeating_item_marker() -> crate::Result<()> {
        let dir = tempdir()?;
        let path = dir.path().join("0");

        let values = [
            BatchItem::new("default", *b"abc", *b"def", ValueType::Value),
            BatchItem::new("default", *b"yxc", *b"ghj", ValueType::Value),
        ];

        {
            let journal = Journal::create_new(&path)?;
            journal
                .get_writer()
                .write_batch(values.iter(), values.len(), 0)?;
        }

        {
            let journal = Journal::from_file(&path)?;
            let reader = journal.get_reader()?;
            let collected = reader.flatten().collect::<Vec<_>>();
            assert_eq!(values.to_vec(), collected.first().unwrap().items);
        }

        // Mangle journal
        {
            let mut file = std::fs::OpenOptions::new().append(true).open(&path)?;
            Marker::Item {
                partition: "default".into(),
                key: (*b"zzz").into(),
                value: (*b"").into(),
                value_type: ValueType::Tombstone,
            }
            .encode_into(&mut file)?;

            file.sync_all()?;
        }

        for _ in 0..10 {
            let journal = Journal::from_file(&path)?;
            let reader = journal.get_reader()?;
            let collected = reader.flatten().collect::<Vec<_>>();
            assert_eq!(values.to_vec(), collected.first().unwrap().items);
        }

        // Mangle journal
        for _ in 0..5 {
            let mut file = std::fs::OpenOptions::new().append(true).open(&path)?;
            Marker::Item {
                partition: "default".into(),
                key: (*b"zzz").into(),
                value: (*b"").into(),
                value_type: ValueType::Tombstone,
            }
            .encode_into(&mut file)?;

            file.sync_all()?;
        }

        for _ in 0..10 {
            let journal = Journal::from_file(&path)?;
            let reader = journal.get_reader()?;
            let collected = reader.flatten().collect::<Vec<_>>();
            assert_eq!(values.to_vec(), collected.first().unwrap().items);
        }

        Ok(())
    }
}
