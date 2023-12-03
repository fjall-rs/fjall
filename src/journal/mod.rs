use self::shard::JournalShard;
use crate::{
    journal::{marker::Marker, recovery::LogRecovery},
    memtable::MemTable,
    sharded::Sharded,
};
use std::{
    path::{Path, PathBuf},
    sync::{RwLock, RwLockWriteGuard},
};
mod marker;
mod recovery;
pub mod shard;

pub struct Journal {
    pub path: PathBuf,
    pub shards: Sharded<JournalShard>,
}

const SHARD_COUNT: u8 = 4;

fn get_shard_path<P: AsRef<Path>>(base: P, idx: u8) -> PathBuf {
    base.as_ref().join(idx.to_string())
}

impl Journal {
    pub fn recover<P: AsRef<Path>>(path: P) -> crate::Result<(Self, MemTable)> {
        log::info!("Recovering journal from {}", path.as_ref().display());

        let path = path.as_ref();

        let memtable = MemTable::default();

        for idx in 0..SHARD_COUNT {
            let shard_path = get_shard_path(path, idx);

            if shard_path.exists() {
                let recoverer = LogRecovery::new(shard_path)?;

                for item in recoverer {
                    let item = item?;

                    if let Marker::Item(item) = item {
                        memtable.insert(item);
                    }
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
