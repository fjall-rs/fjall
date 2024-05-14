pub mod manager;
mod marker;
pub mod partition_manifest;
mod reader;
pub mod shard;
pub mod writer;

use self::{
    shard::{JournalShard, RecoveryMode},
    writer::FlushMode,
};
use crate::{batch::PartitionKey, file::fsync_directory, sharded::Sharded};
use lsm_tree::MemTable;
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{RwLock, RwLockWriteGuard},
};

const SHARD_COUNT: u8 = 4;

fn get_shard_path<P: AsRef<Path>>(base: P, idx: u8) -> PathBuf {
    base.as_ref().join(idx.to_string())
}

pub struct Journal {
    pub path: PathBuf,
    pub shards: Sharded<JournalShard>,
}

impl Journal {
    pub fn recover_memtables<P: AsRef<Path>>(
        path: P,
        whitelist: Option<&[PartitionKey]>,
        recovery_mode: RecoveryMode,
    ) -> crate::Result<HashMap<PartitionKey, MemTable>> {
        let path = path.as_ref();
        let mut memtables = HashMap::new();

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
    }

    pub fn recover<P: AsRef<Path>>(
        path: P,
        recovery_mode: RecoveryMode,
    ) -> crate::Result<(Self, HashMap<PartitionKey, MemTable>)> {
        let path = path.as_ref();
        log::info!("Recovering journal from {path:?}");

        let memtables = Self::recover_memtables(path, None, recovery_mode)?;

        let shards = (0..SHARD_COUNT)
            .map(|idx| {
                Ok(RwLock::new(JournalShard::from_file(get_shard_path(
                    path, idx,
                ))?))
            })
            .collect::<crate::Result<Vec<_>>>()?;

        Ok((
            Self {
                shards: Sharded::new(shards),
                path: path.to_path_buf(),
            },
            memtables,
        ))
    }

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

        Ok(Self {
            shards: Sharded::new(shards),
            path: path.to_path_buf(),
        })
    }

    pub(crate) fn get_writer(&self) -> RwLockWriteGuard<'_, JournalShard> {
        let mut shard = self.shards.write_one();
        shard.should_sync = true;
        shard
    }

    pub fn flush(&self, mode: FlushMode) -> crate::Result<()> {
        for mut shard in self.shards.full_lock().expect("lock is poisoned") {
            if shard.should_sync {
                shard.writer.flush(mode);
                shard.should_sync = false;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
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
                key: "zzz".as_bytes().into(),
                value: "".as_bytes().into(),
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
                key: "zzz".as_bytes().into(),
                value: "".as_bytes().into(),
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
