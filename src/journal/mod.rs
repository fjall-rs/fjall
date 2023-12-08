mod marker;
mod recovery;
pub mod shard;

use self::shard::JournalShard;
use crate::{
    journal::{marker::Marker, recovery::JournalRecovery},
    memtable::MemTable,
    sharded::Sharded,
    value::SeqNo,
};
use std::{
    fs::OpenOptions,
    path::{Path, PathBuf},
    sync::{RwLock, RwLockWriteGuard},
};

const SHARD_COUNT: u8 = 4;

fn get_shard_path<P: AsRef<Path>>(base: P, idx: u8) -> PathBuf {
    base.as_ref().join(idx.to_string())
}

// TODO: strategy, skip invalid batches (CRC or invalid item length) or throw error
/// Errors that can occur during journal recovery
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RecoveryError {
    /// Batch had less items than expected, so it's incomplete
    InsufficientLength,

    /// Batch was not terminated, so it's possibly incomplete
    MissingTerminator,

    /// The CRC value does not match the expected value
    CrcCheck,
}

pub struct Journal {
    pub path: PathBuf,
    pub shards: Sharded<JournalShard>,
}

impl Journal {
    pub fn recover<P: AsRef<Path>>(path: P) -> crate::Result<(Self, MemTable)> {
        log::info!("Recovering journal from {}", path.as_ref().display());

        let path = path.as_ref();

        let memtable = MemTable::default();

        for idx in 0..SHARD_COUNT {
            //  TODO: move this loop body into JournalShard::recover

            let shard_path = get_shard_path(path, idx);

            if shard_path.exists() {
                let recoverer = JournalRecovery::new(shard_path.clone())?;

                let mut hasher = crc32fast::Hasher::new();
                let mut is_in_batch = false;
                let mut batch_counter = 0;
                let mut batch_seqno = SeqNo::default();
                let mut last_valid_pos = 0;

                let mut items = vec![];

                'a: for item in recoverer {
                    log::error!("{item:?}");
                    let (journal_file_pos, item) = item?;

                    match item {
                        Marker::Start { item_count, seqno } => {
                            if is_in_batch {
                                log::warn!("Invalid batch: found batch start inside batch");
                                /* return Err(crate::Error::JournalRecovery(
                                    RecoveryError::MissingTerminator,
                                )); */
                                log::warn!("Truncating shard to {last_valid_pos}");
                                let file = OpenOptions::new().write(true).open(&shard_path)?;
                                file.set_len(last_valid_pos)?;
                                file.sync_all()?;

                                break 'a;
                            }

                            is_in_batch = true;
                            batch_counter = item_count;
                            batch_seqno = seqno;
                        }
                        Marker::End(_checksum) => {
                            if batch_counter > 0 {
                                log::error!("Invalid batch: insufficient length");
                                return Err(crate::Error::JournalRecovery(
                                    RecoveryError::InsufficientLength,
                                ));
                                // break 'a;
                            }

                            // TODO:
                            /*  if hasher.finalize() != checksum {
                                log::error!("Invalid batch: checksum check failed");
                                todo!("return Err or discard");
                            } */

                            if !is_in_batch {
                                log::error!("Invalid batch: found end marker without start marker");

                                log::warn!("Truncating shard to {last_valid_pos}");
                                let file = OpenOptions::new().write(true).open(&shard_path)?;
                                file.set_len(last_valid_pos)?;
                                file.sync_all()?;

                                break 'a;
                            }

                            hasher = crc32fast::Hasher::new();
                            is_in_batch = false;
                            batch_counter = 0;

                            // NOTE: Clippy says into_iter() is better
                            // but in this case probably not
                            #[allow(clippy::iter_with_drain)]
                            for item in items.drain(..) {
                                memtable.insert(item);
                            }

                            last_valid_pos = journal_file_pos;
                        }
                        Marker::Item {
                            key,
                            value,
                            value_type,
                        } => {
                            // TODO: CRC
                            // byte_count += bytes.len() as u64;
                            // hasher.update(bytes);

                            // TODO: check batch counter == 0

                            if !is_in_batch {
                                log::error!("Invalid batch: found end marker without start marker");

                                log::warn!("Truncating shard to {last_valid_pos}");
                                let file = OpenOptions::new().write(true).open(&shard_path)?;
                                file.set_len(last_valid_pos)?;
                                file.sync_all()?;

                                break 'a;
                            }

                            batch_counter -= 1;

                            items.push(crate::Value {
                                key,
                                value,
                                seqno: batch_seqno,
                                value_type,
                            });
                        }
                    }
                }

                if is_in_batch {
                    /*  return Err(crate::Error::JournalRecovery(
                        RecoveryError::MissingTerminator,
                    )); */
                    log::warn!("Invalid batch: missing terminator, but last batch, so probably incomplete, discarding to keep atomicity");
                    // Discard item

                    log::warn!("Truncating shard to {last_valid_pos}");
                    let file = OpenOptions::new().write(true).open(&shard_path)?;
                    file.set_len(last_valid_pos)?;
                    file.sync_all()?;
                }

                log::trace!("Recovered journal shard {idx}");
            }
        }

        let shards = (0..SHARD_COUNT)
            .map(|idx| {
                Ok(RwLock::new(JournalShard::from_file(get_shard_path(
                    path, idx,
                ))?))
            })
            .collect::<crate::Result<Vec<_>>>()?;

        log::info!("Recovered all journal shards");

        Ok((
            Self {
                shards: Sharded::new(shards),
                path: path.to_path_buf(),
            },
            memtable,
        ))
    }

    pub fn rotate<P: AsRef<Path>>(
        path: P,
        shards: &mut [RwLockWriteGuard<'_, JournalShard>],
    ) -> crate::Result<()> {
        log::info!("Rotating active journal to {}", path.as_ref().display());

        let path = path.as_ref();

        std::fs::create_dir_all(path)?;

        for (idx, shard) in shards.iter_mut().enumerate() {
            shard.rotate(path.join(idx.to_string()))?;
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

        Ok(Self {
            shards: Sharded::new(shards),
            path: path.to_path_buf(),
        })
    }

    pub(crate) fn lock_shard(&self) -> RwLockWriteGuard<'_, JournalShard> {
        self.shards.write_one()
    }

    pub fn flush(&self) -> crate::Result<()> {
        for mut shard in self.shards.full_lock().expect("lock is poisoned") {
            shard.flush()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{serde::Serializable, value::ValueType, Value};
    use std::io::Write;
    use tempfile::tempdir;
    use test_log::test;

    #[test]
    fn test_log_truncation_corrupt_bytes() -> crate::Result<()> {
        let dir = tempdir()?;
        let shard_path = dir.path().join("0");

        let values = [
            &Value::new(*b"abc", *b"def", 0, ValueType::Value),
            &Value::new(*b"yxc", *b"ghj", 1, ValueType::Value),
        ];

        {
            let mut shard = JournalShard::create_new(&shard_path)?;
            shard.write_batch(&values)?;
        }

        let file_size_before_mangle = std::fs::metadata(&shard_path)?.len();

        {
            let (_, memtable) = Journal::recover(&dir)?;
            assert_eq!(memtable.items.len(), values.len());
        }

        // Mangle journal
        {
            let mut file = std::fs::OpenOptions::new().append(true).open(&shard_path)?;
            file.write_all(b"09pmu35w3a9mp53bao9upw3ab5up")?;
            file.sync_all()?;
            assert!(file.metadata()?.len() > file_size_before_mangle);
        }

        for _ in 0..10 {
            let (_, memtable) = Journal::recover(&dir)?;

            // Should recover all items
            assert_eq!(memtable.items.len(), values.len());

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
            let (_, memtable) = Journal::recover(&dir)?;

            // Should recover all items
            assert_eq!(memtable.items.len(), values.len());

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
            &Value::new(*b"abc", *b"def", 0, ValueType::Value),
            &Value::new(*b"yxc", *b"ghj", 1, ValueType::Value),
        ];

        {
            let mut shard = JournalShard::create_new(&shard_path)?;
            shard.write_batch(&values)?;
        }

        let file_size_before_mangle = std::fs::metadata(&shard_path)?.len();

        {
            let (_, memtable) = Journal::recover(&dir)?;
            assert_eq!(memtable.items.len(), values.len());
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
            let (_, memtable) = Journal::recover(&dir)?;

            // Should recover all items
            assert_eq!(memtable.items.len(), values.len());

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
            let (_, memtable) = Journal::recover(&dir)?;

            // Should recover all items
            assert_eq!(memtable.items.len(), values.len());

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
            &Value::new(*b"abc", *b"def", 0, ValueType::Value),
            &Value::new(*b"yxc", *b"ghj", 1, ValueType::Value),
        ];

        {
            let mut shard = JournalShard::create_new(&shard_path)?;
            shard.write_batch(&values)?;
        }

        let file_size_before_mangle = std::fs::metadata(&shard_path)?.len();

        {
            let (_, memtable) = Journal::recover(&dir)?;
            assert_eq!(memtable.items.len(), values.len());
        }

        // Mangle journal
        {
            let mut file = std::fs::OpenOptions::new().append(true).open(&shard_path)?;
            Marker::End(5432).serialize(&mut file)?;
            file.sync_all()?;
            assert!(file.metadata()?.len() > file_size_before_mangle);
        }

        for _ in 0..10 {
            let (_, memtable) = Journal::recover(&dir)?;

            // Should recover all items
            assert_eq!(memtable.items.len(), values.len());

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
            let (_, memtable) = Journal::recover(&dir)?;

            // Should recover all items
            assert_eq!(memtable.items.len(), values.len());

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
            &Value::new(*b"abc", *b"def", 0, ValueType::Value),
            &Value::new(*b"yxc", *b"ghj", 1, ValueType::Value),
        ];

        {
            let mut shard = JournalShard::create_new(&shard_path)?;
            shard.write_batch(&values)?;
        }

        let file_size_before_mangle = std::fs::metadata(&shard_path)?.len();

        {
            let (_, memtable) = Journal::recover(&dir)?;
            assert_eq!(memtable.items.len(), values.len());
        }

        // Mangle journal
        {
            let mut file = std::fs::OpenOptions::new().append(true).open(&shard_path)?;
            Marker::Item {
                key: "zzz".as_bytes().into(),
                value: "".as_bytes().into(),
                value_type: ValueType::Tombstone,
            }
            .serialize(&mut file)?;

            file.sync_all()?;
            assert!(file.metadata()?.len() > file_size_before_mangle);
        }

        for _ in 0..10 {
            let (_, memtable) = Journal::recover(&dir)?;

            // Should recover all items
            assert_eq!(memtable.items.len(), values.len());

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
                key: "zzz".as_bytes().into(),
                value: "".as_bytes().into(),
                value_type: ValueType::Tombstone,
            }
            .serialize(&mut file)?;

            file.sync_all()?;
            assert!(file.metadata()?.len() > file_size_before_mangle);
        }

        for _ in 0..10 {
            let (_, memtable) = Journal::recover(&dir)?;

            // Should recover all items
            assert_eq!(memtable.items.len(), values.len());

            // Should truncate to before-mangled state
            assert_eq!(
                std::fs::metadata(&shard_path)?.len(),
                file_size_before_mangle
            );
        }

        Ok(())
    }
}
