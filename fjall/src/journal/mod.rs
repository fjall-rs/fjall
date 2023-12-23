pub mod manager;
mod marker;
mod recovery;
pub mod shard;
pub mod writer;

use self::shard::JournalShard;
use crate::sharded::Sharded;
use lsm_tree::MemTable;
use std::{
    collections::HashMap,
    fs::File,
    path::{Path, PathBuf},
    sync::{Arc, RwLock, RwLockWriteGuard},
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
    pub fn recover<P: AsRef<Path>>(path: P) -> crate::Result<(Self, HashMap<Arc<str>, MemTable>)> {
        log::info!("Recovering journal from {}", path.as_ref().display());

        let path = path.as_ref();

        let mut memtables = HashMap::new();

        for idx in 0..SHARD_COUNT {
            let shard_path = get_shard_path(path, idx);

            if shard_path.exists() {
                JournalShard::recover_and_repair(shard_path, &mut memtables)?;
                log::trace!("Recovered journal shard");
            } else {
                log::trace!("Journal shard file does not exist (yet)");
            }
        }

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
        log::debug!("Rotating active journal to {}", path.as_ref().display());

        let path = path.as_ref();

        std::fs::create_dir_all(path)?;

        for (idx, shard) in shards.iter_mut().enumerate() {
            shard.rotate(path.join(idx.to_string()))?;
        }

        #[cfg(not(target_os = "windows"))]
        {
            // Fsync folder on Unix
            let folder = File::open(path)?;
            folder.sync_all()?;
        }

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

        #[cfg(not(target_os = "windows"))]
        {
            // Fsync folder on Unix
            let folder = std::fs::File::open(path)?;
            folder.sync_all()?;
        }

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

    pub fn flush(&self) -> crate::Result<()> {
        for mut shard in self.shards.full_lock().expect("lock is poisoned") {
            if shard.should_sync {
                shard.writer.flush()?;
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

        let file_size_before_mangle = std::fs::metadata(&shard_path)?.len();

        {
            let (_, memtables) = Journal::recover(&dir)?;
            let memtable = memtables.get("default").expect("should exist");
            assert_eq!(memtable.len(), values.len());
        }

        // Mangle journal
        {
            let mut file = std::fs::OpenOptions::new().append(true).open(&shard_path)?;
            file.write_all(b"09pmu35w3a9mp53bao9upw3ab5up")?;
            file.sync_all()?;
            assert!(file.metadata()?.len() > file_size_before_mangle);
        }

        for _ in 0..10 {
            let (_, memtables) = Journal::recover(&dir)?;
            let memtable = memtables.get("default").expect("should exist");

            // Should recover all items
            assert_eq!(memtable.len(), values.len());

            // Should truncate to before-mangled state
            assert_eq!(
                std::fs::metadata(&shard_path)?.len(),
                file_size_before_mangle
            );
        }

        // Mangle journal
        for _ in 0..5 {
            let mut file = std::fs::OpenOptions::new().append(true).open(&shard_path)?;
            file.write_all(b"09pmu35w3a9mp53bao9upw3ab5up")?;
            file.sync_all()?;
            assert!(file.metadata()?.len() > file_size_before_mangle);
        }

        for _ in 0..10 {
            let (_, memtables) = Journal::recover(&dir)?;
            let memtable = memtables.get("default").expect("should exist");

            // Should recover all items
            assert_eq!(memtable.len(), values.len());

            // Should truncate to before-mangled state
            assert_eq!(
                std::fs::metadata(&shard_path)?.len(),
                file_size_before_mangle
            );
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

        let file_size_before_mangle = std::fs::metadata(&shard_path)?.len();

        {
            let (_, memtables) = Journal::recover(&dir)?;
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
            assert!(file.metadata()?.len() > file_size_before_mangle);
        }

        for _ in 0..10 {
            let (_, memtables) = Journal::recover(&dir)?;
            let memtable = memtables.get("default").expect("should exist");

            // Should recover all items
            assert_eq!(memtable.len(), values.len());

            // Should truncate to before-mangled state
            assert_eq!(
                std::fs::metadata(&shard_path)?.len(),
                file_size_before_mangle
            );
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
            assert!(file.metadata()?.len() > file_size_before_mangle);
        }

        for _ in 0..10 {
            let (_, memtables) = Journal::recover(&dir)?;
            let memtable = memtables.get("default").expect("should exist");

            // Should recover all items
            assert_eq!(memtable.len(), values.len());

            // Should truncate to before-mangled state
            assert_eq!(
                std::fs::metadata(&shard_path)?.len(),
                file_size_before_mangle
            );
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

        let file_size_before_mangle = std::fs::metadata(&shard_path)?.len();

        {
            let (_, memtables) = Journal::recover(&dir)?;
            let memtable = memtables.get("default").expect("should exist");

            assert_eq!(memtable.len(), values.len());
        }

        // Mangle journal
        {
            let mut file = std::fs::OpenOptions::new().append(true).open(&shard_path)?;
            Marker::End(5432).serialize(&mut file)?;
            file.sync_all()?;
            assert!(file.metadata()?.len() > file_size_before_mangle);
        }

        for _ in 0..10 {
            let (_, memtables) = Journal::recover(&dir)?;
            let memtable = memtables.get("default").expect("should exist");

            // Should recover all items
            assert_eq!(memtable.len(), values.len());

            // Should truncate to before-mangled state
            assert_eq!(
                std::fs::metadata(&shard_path)?.len(),
                file_size_before_mangle
            );
        }

        // Mangle journal
        for _ in 0..5 {
            let mut file = std::fs::OpenOptions::new().append(true).open(&shard_path)?;
            Marker::End(5432).serialize(&mut file)?;
            file.sync_all()?;
            assert!(file.metadata()?.len() > file_size_before_mangle);
        }

        for _ in 0..10 {
            let (_, memtables) = Journal::recover(&dir)?;
            let memtable = memtables.get("default").expect("should exist");

            // Should recover all items
            assert_eq!(memtable.len(), values.len());

            // Should truncate to before-mangled state
            assert_eq!(
                std::fs::metadata(&shard_path)?.len(),
                file_size_before_mangle
            );
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

        let file_size_before_mangle = std::fs::metadata(&shard_path)?.len();

        {
            let (_, memtables) = Journal::recover(&dir)?;
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
            assert!(file.metadata()?.len() > file_size_before_mangle);
        }

        for _ in 0..10 {
            let (_, memtables) = Journal::recover(&dir)?;
            let memtable = memtables.get("default").expect("should exist");

            // Should recover all items
            assert_eq!(memtable.len(), values.len());

            // Should truncate to before-mangled state
            assert_eq!(
                std::fs::metadata(&shard_path)?.len(),
                file_size_before_mangle
            );
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
            assert!(file.metadata()?.len() > file_size_before_mangle);
        }

        for _ in 0..10 {
            let (_, memtables) = Journal::recover(&dir)?;
            let memtable = memtables.get("default").expect("should exist");

            // Should recover all items
            assert_eq!(memtable.len(), values.len());

            // Should truncate to before-mangled state
            assert_eq!(
                std::fs::metadata(&shard_path)?.len(),
                file_size_before_mangle
            );
        }

        Ok(())
    }
}
