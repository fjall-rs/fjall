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
            let shard_path = get_shard_path(path, idx);

            if shard_path.exists() {
                let recoverer = JournalRecovery::new(shard_path)?;

                let mut hasher = crc32fast::Hasher::new();
                let mut is_in_batch = false;
                let mut batch_counter = 0;
                let mut batch_seqno = SeqNo::default();

                let mut items = vec![];

                for item in recoverer {
                    let item = item?;

                    match item {
                        Marker::Start { item_count, seqno } => {
                            if is_in_batch {
                                log::error!("Invalid batch: found batch start inside batch");
                                return Err(crate::Error::JournalRecovery(
                                    RecoveryError::MissingTerminator,
                                ));
                            }

                            is_in_batch = true;
                            batch_counter = item_count;
                            batch_seqno = seqno;
                        }
                        Marker::End(_checksum) => {
                            if batch_counter > 0 {
                                return Err(crate::Error::JournalRecovery(
                                    RecoveryError::InsufficientLength,
                                ));
                            }

                            // TODO:
                            /*  if hasher.finalize() != checksum {
                                log::error!("Invalid batch: checksum check failed");
                                todo!("return Err or discard");
                            } */

                            hasher = crc32fast::Hasher::new();
                            is_in_batch = false;
                            batch_counter = 0;

                            // NOTE: Clippy says into_iter() is better
                            // but in this case probably not
                            #[allow(clippy::iter_with_drain)]
                            for item in items.drain(..) {
                                memtable.insert(item);
                            }
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
                    return Err(crate::Error::JournalRecovery(
                        RecoveryError::MissingTerminator,
                    ));
                }

                log::trace!("Recovered journal shard {idx}");
            }
        }

        let shards = (0..SHARD_COUNT)
            .map(|idx| {
                Ok(RwLock::new(JournalShard::recover(get_shard_path(
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
        for mut shard in self.shards.full_lock() {
            shard.flush()?;
        }
        Ok(())
    }
}
