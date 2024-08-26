// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

pub mod manager;
mod marker;
pub mod partition_manifest;
mod reader;
pub mod shard;
pub mod writer;

use self::{shard::JournalShard, writer::PersistMode};
use crate::{file::fsync_directory, sharded::Sharded};
use reader::JournalReader;
use std::{
    path::{Path, PathBuf},
    sync::{RwLock, RwLockWriteGuard},
};

pub const SHARD_COUNT: u8 = 4;

fn get_shard_path<P: AsRef<Path>>(base: P, idx: u8) -> PathBuf {
    base.as_ref().join(idx.to_string())
}

pub struct Journal {
    pub path: PathBuf,
    shards: Sharded<JournalShard>,
}

impl Drop for Journal {
    fn drop(&mut self) {
        log::trace!("Dropping journal, trying to flush");

        match self.flush(PersistMode::SyncAll) {
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

// TODO: need JournalReader which merges all shards
// TODO: into a single stream... the stream emitting
// TODO: valid batches, deprecates Shard::recover_and_repair
// TODO: need to write the valid batches into a RecoveryThing
// TODO: that writes into the memtables
// TODO: the journal shouldn't know about memtables!!!

impl Journal {
    pub fn get_reader<P: AsRef<Path>>(path: P) -> crate::Result<JournalReader> {
        JournalReader::new(path)
    }

    pub fn restore_existing<P: AsRef<Path>>(path: P) -> crate::Result<Self> {
        let shards = (0..SHARD_COUNT)
            .map(|idx| {
                Ok(RwLock::new(JournalShard::from_file(get_shard_path(
                    &path, idx,
                ))?))
            })
            .collect::<crate::Result<Vec<_>>>()?;

        Ok(Self {
            path: path.as_ref().into(),
            shards: Sharded::new(shards),
        })
    }

    /* pub fn recover_memtables<P: AsRef<Path>>(
        path: P,
        whitelist: Option<&[PartitionKey]>,
        recovery_mode: RecoveryMode,
    ) -> crate::Result<HashMap<PartitionKey, Memtable>> {
        let path = path.as_ref();
        let mut memtables = HashMap::with_hasher(xxhash_rust::xxh3::Xxh3Builder::new());

        for idx in 0..SHARD_COUNT {
            let shard_path = get_shard_path(path, idx);

            if shard_path.exists() {
                JournalShard::recover_and_repair(
                    shard_path,
                    &mut memtables,
                    whitelist,
                    recovery_mode,
                )?;
                log::trace!("Recovered journal shard");
            } else {
                log::trace!("Journal shard file does not exist (yet)");
            }
        }

        Ok(memtables)
    } */

    /* pub fn recover<P: AsRef<Path>>(
        path: P,
        recovery_mode: RecoveryMode,
    ) -> crate::Result<(Self, HashMap<PartitionKey, Memtable>)> {
        let path = path.as_ref();
        log::debug!("Recovering journal from {path:?}");

        let memtables = Self::recover_memtables(path, None, recovery_mode)?;

        let shards = (0..SHARD_COUNT)
            .map(|idx| {
                Ok(RwLock::new(JournalShard::from_file(get_shard_path(
                    path, idx,
                ))?))
            })
            .collect::<crate::Result<Vec<_>>>()?;

        #[cfg(feature = "__internal_whitebox")]
        crate::drop::increment_drop_counter();

        Ok((
            Self {
                shards: Sharded::new(shards),
                path: path.to_path_buf(),
            },
            memtables,
        ))
    } */

    /*   pub fn recover<P: AsRef<Path>>(
        path: P,
        recovery_mode: RecoveryMode,
    ) -> crate::Result<(Self, HashMap<PartitionKey, Memtable>)> {
        let path = path.as_ref();
        log::debug!("Recovering journal from {path:?}");

        let memtables = Self::recover_memtables(path, None, recovery_mode)?;

        let shards = (0..SHARD_COUNT)
            .map(|idx| {
                Ok(RwLock::new(JournalShard::from_file(get_shard_path(
                    path, idx,
                ))?))
            })
            .collect::<crate::Result<Vec<_>>>()?;

        #[cfg(feature = "__internal_whitebox")]
        crate::drop::increment_drop_counter();

        Ok((
            Self {
                shards: Sharded::new(shards),
                path: path.to_path_buf(),
            },
            memtables,
        ))
    } */

    pub fn rotate<P: AsRef<Path>>(
        path: P,
        shards: &mut [RwLockWriteGuard<'_, JournalShard>],
    ) -> crate::Result<()> {
        let path = path.as_ref();

        log::debug!("Rotating active journal to {path:?}");

        std::fs::create_dir_all(path)?;

        for (idx, shard) in shards.iter_mut().enumerate() {
            shard.rotate(path.join(idx.to_string()))?;
        }

        // IMPORTANT: fsync folder on Unix
        fsync_directory(path)?;

        Ok(())
    }

    pub fn create_new<P: AsRef<Path>>(path: P) -> crate::Result<Self> {
        let path = path.as_ref();

        std::fs::create_dir_all(path)?;

        let shards = (0..SHARD_COUNT)
            .map(|idx| {
                Ok(RwLock::new(JournalShard::create_new(get_shard_path(
                    path, idx,
                ))?))
            })
            .collect::<crate::Result<Vec<_>>>()?;

        // IMPORTANT: fsync folder on Unix
        fsync_directory(path)?;

        #[cfg(feature = "__internal_whitebox")]
        crate::drop::increment_drop_counter();

        Ok(Self {
            shards: Sharded::new(shards),
            path: path.to_path_buf(),
        })
    }

    /// Locks all shards for exclusive control over the journal.
    pub(crate) fn full_lock(&self) -> Vec<RwLockWriteGuard<'_, JournalShard>> {
        self.shards.full_lock().expect("lock is poisoned")
    }

    /// Locks a shard to write to it.
    pub(crate) fn get_writer(&self) -> RwLockWriteGuard<'_, JournalShard> {
        let mut shard = self.shards.write_one();
        shard.should_sync = true;
        shard
    }

    /// Flushes the journal.
    pub fn flush(&self, mode: PersistMode) -> crate::Result<()> {
        for mut shard in self.full_lock() {
            if shard.should_sync {
                shard.writer.flush(mode)?;
                shard.should_sync = false;
            }
        }
        Ok(())
    }
}

/* #[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::marker::Marker;
    use super::*;
    use crate::batch::item::Item as BatchItem;
    use lsm_tree::{serde::Serializable, ValueType};
    use std::io::Write;
    use tempfile::tempdir;
    use test_log::test;

    #[test]
    fn test_log_truncation_corrupt_bytes() -> crate::Result<()> {
        let dir = tempdir()?;
        let shard_path = dir.path().join("0");

        let values = [
            &BatchItem::new("default", *b"abc", *b"def", ValueType::Value),
            &BatchItem::new("default", *b"yxc", *b"ghj", ValueType::Value),
        ];

        {
            let mut shard = JournalShard::create_new(&shard_path)?;
            shard.writer.write_batch(&values, 0)?;
        }

        {
            let (_, memtables) = Journal::recover(&dir, RecoveryMode::TolerateCorruptTail)?;
            let memtable = memtables.get("default").expect("should exist");
            assert_eq!(memtable.len(), values.len());
        }

        // Mangle journal
        {
            let mut file = std::fs::OpenOptions::new().append(true).open(&shard_path)?;
            file.write_all(b"09pmu35w3a9mp53bao9upw3ab5up")?;
            file.sync_all()?;
        }

        for _ in 0..10 {
            let (_, memtables) = Journal::recover(&dir, RecoveryMode::TolerateCorruptTail)?;
            let memtable = memtables.get("default").expect("should exist");

            // Should recover all items
            assert_eq!(memtable.len(), values.len());
        }

        // Mangle journal
        for _ in 0..5 {
            let mut file = std::fs::OpenOptions::new().append(true).open(&shard_path)?;
            file.write_all(b"09pmu35w3a9mp53bao9upw3ab5up")?;
            file.sync_all()?;
        }

        for _ in 0..10 {
            let (_, memtables) = Journal::recover(&dir, RecoveryMode::TolerateCorruptTail)?;
            let memtable = memtables.get("default").expect("should exist");

            // Should recover all items
            assert_eq!(memtable.len(), values.len());
        }

        Ok(())
    }

    #[test]
    fn test_log_truncation_repeating_start_marker() -> crate::Result<()> {
        let dir = tempdir()?;
        let shard_path = dir.path().join("0");

        let values = [
            &BatchItem::new("default", *b"abc", *b"def", ValueType::Value),
            &BatchItem::new("default", *b"yxc", *b"ghj", ValueType::Value),
        ];

        {
            let mut shard = JournalShard::create_new(&shard_path)?;
            shard.writer.write_batch(&values, 0)?;
        }

        {
            let (_, memtables) = Journal::recover(&dir, RecoveryMode::TolerateCorruptTail)?;
            let memtable = memtables.get("default").expect("should exist");

            assert_eq!(memtable.len(), values.len());
        }

        // Mangle journal
        {
            let mut file = std::fs::OpenOptions::new().append(true).open(&shard_path)?;
            Marker::Start {
                item_count: 2,
                seqno: 64,
                compression: lsm_tree::CompressionType::None,
            }
            .serialize(&mut file)?;
            file.sync_all()?;
        }

        for _ in 0..10 {
            let (_, memtables) = Journal::recover(&dir, RecoveryMode::TolerateCorruptTail)?;
            let memtable = memtables.get("default").expect("should exist");

            // Should recover all items
            assert_eq!(memtable.len(), values.len());
        }

        // Mangle journal
        for _ in 0..5 {
            let mut file = std::fs::OpenOptions::new().append(true).open(&shard_path)?;
            Marker::Start {
                item_count: 2,
                seqno: 64,
                compression: lsm_tree::CompressionType::None,
            }
            .serialize(&mut file)?;
            file.sync_all()?;
        }

        for _ in 0..10 {
            let (_, memtables) = Journal::recover(&dir, RecoveryMode::TolerateCorruptTail)?;
            let memtable = memtables.get("default").expect("should exist");

            // Should recover all items
            assert_eq!(memtable.len(), values.len());
        }

        Ok(())
    }

    #[test]
    fn test_log_truncation_repeating_end_marker() -> crate::Result<()> {
        let dir = tempdir()?;
        let shard_path = dir.path().join("0");

        let values = [
            &BatchItem::new("default", *b"abc", *b"def", ValueType::Value),
            &BatchItem::new("default", *b"yxc", *b"ghj", ValueType::Value),
        ];

        {
            let mut shard = JournalShard::create_new(&shard_path)?;
            shard.writer.write_batch(&values, 0)?;
        }

        {
            let (_, memtables) = Journal::recover(&dir, RecoveryMode::TolerateCorruptTail)?;
            let memtable = memtables.get("default").expect("should exist");

            assert_eq!(memtable.len(), values.len());
        }

        // Mangle journal
        {
            let mut file = std::fs::OpenOptions::new().append(true).open(&shard_path)?;
            Marker::End(5432).serialize(&mut file)?;
            file.sync_all()?;
        }

        for _ in 0..10 {
            let (_, memtables) = Journal::recover(&dir, RecoveryMode::TolerateCorruptTail)?;
            let memtable = memtables.get("default").expect("should exist");

            // Should recover all items
            assert_eq!(memtable.len(), values.len());
        }

        // Mangle journal
        for _ in 0..5 {
            let mut file = std::fs::OpenOptions::new().append(true).open(&shard_path)?;
            Marker::End(5432).serialize(&mut file)?;
            file.sync_all()?;
        }

        for _ in 0..10 {
            let (_, memtables) = Journal::recover(&dir, RecoveryMode::TolerateCorruptTail)?;
            let memtable = memtables.get("default").expect("should exist");

            // Should recover all items
            assert_eq!(memtable.len(), values.len());
        }

        Ok(())
    }

    #[test]
    fn test_log_truncation_repeating_item_marker() -> crate::Result<()> {
        let dir = tempdir()?;
        let shard_path = dir.path().join("0");

        let values = [
            &BatchItem::new("default", *b"abc", *b"def", ValueType::Value),
            &BatchItem::new("default", *b"yxc", *b"ghj", ValueType::Value),
        ];

        {
            let mut shard = JournalShard::create_new(&shard_path)?;
            shard.writer.write_batch(&values, 0)?;
        }

        {
            let (_, memtables) = Journal::recover(&dir, RecoveryMode::TolerateCorruptTail)?;
            let memtable = memtables.get("default").expect("should exist");

            assert_eq!(memtable.len(), values.len());
        }

        // Mangle journal
        {
            let mut file = std::fs::OpenOptions::new().append(true).open(&shard_path)?;
            Marker::Item {
                partition: "default".into(),
                key: (*b"zzz").into(),
                value: (*b"").into(),
                value_type: ValueType::Tombstone,
            }
            .serialize(&mut file)?;

            file.sync_all()?;
        }

        for _ in 0..10 {
            let (_, memtables) = Journal::recover(&dir, RecoveryMode::TolerateCorruptTail)?;
            let memtable = memtables.get("default").expect("should exist");

            // Should recover all items
            assert_eq!(memtable.len(), values.len());
        }

        // Mangle journal
        for _ in 0..5 {
            let mut file = std::fs::OpenOptions::new().append(true).open(&shard_path)?;
            Marker::Item {
                partition: "default".into(),
                key: (*b"zzz").into(),
                value: (*b"").into(),
                value_type: ValueType::Tombstone,
            }
            .serialize(&mut file)?;

            file.sync_all()?;
        }

        for _ in 0..10 {
            let (_, memtables) = Journal::recover(&dir, RecoveryMode::TolerateCorruptTail)?;
            let memtable = memtables.get("default").expect("should exist");

            // Should recover all items
            assert_eq!(memtable.len(), values.len());
        }

        Ok(())
    }
}
 */
