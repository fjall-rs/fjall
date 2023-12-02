use self::shard::JournalShard;
use crate::{sharded::Sharded, Value};
use std::{
    path::{Path, PathBuf},
    sync::{RwLock, RwLockWriteGuard},
};
mod marker;
pub mod mem_table;
pub mod rebuild;
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
    pub fn new<P: AsRef<Path>>(path: P) -> crate::Result<Self> {
        if path.as_ref().exists() {
            Self::recover(path)
        } else {
            Self::create_new(path)
        }
    }

    fn recover<P: AsRef<Path>>(path: P) -> crate::Result<Self> {
        log::info!("Recovering journal from {}", path.as_ref().display());

        let path = path.as_ref();

        // NOTE: Don't listen to clippy!
        // We need to collect the threads
        #[allow(clippy::needless_collect)]
        let shards = (0..SHARD_COUNT)
            .map(|idx| {
                let shard_path = get_shard_path(path, idx);
                std::thread::spawn(move || {
                    Ok::<_, crate::Error>(RwLock::new(JournalShard::recover(shard_path)?))
                })
            })
            .collect::<Vec<_>>();

        let shards = shards
            .into_iter()
            .map(|t| {
                let shard = t.join().expect("should join")?;
                log::debug!("Recovered journal shard");
                Ok(shard)
            })
            .collect::<crate::Result<Vec<_>>>()?;

        log::info!("Recovered all journal shards");

        Ok(Self {
            shards: Sharded::new(shards),
            path: path.to_path_buf(),
        })
    }

    pub fn get_path(&self) -> PathBuf {
        let lock = self.lock_shard();
        lock.path.parent().unwrap().into()
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

        // TODO: OH OH NEED TO RESET PATH HERE

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

    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> Option<Value> {
        let mut item: Option<Value> = None;

        for shard in self.shards.iter() {
            let lock = shard.read().expect("lock is poisoned");

            if let Some(retrieved) = lock.memtable.get(&key) {
                if let Some(inner) = &item {
                    if retrieved.seqno > inner.seqno {
                        item = Some(retrieved);
                    }
                } else {
                    item = Some(retrieved);
                }
            }
        }

        item
    }
}
