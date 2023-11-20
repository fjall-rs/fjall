use crate::{
    block_cache::BlockCache, commit_log::CommitLog, level::Levels, memtable::MemTable, Config,
};
use std::sync::{atomic::AtomicU64, Arc, Mutex, RwLock};

pub struct TreeInner {
    pub(crate) config: Config,
    pub(crate) active_memtable: Arc<RwLock<MemTable>>,
    pub(crate) commit_log: Arc<Mutex<CommitLog>>,
    pub(crate) block_cache: Arc<BlockCache>,
    pub(crate) lsn: AtomicU64,
    pub(crate) levels: Arc<RwLock<Levels>>,
}

impl Drop for TreeInner {
    fn drop(&mut self) {
        log::info!("Dropping TreeInner");

        log::debug!("Trying to flush commit log");
        if let Ok(mut lock) = self.commit_log.lock() {
            if let Err(error) = lock.flush() {
                log::warn!("Failed to flush commit log: {:?}", error);
            }
        }
    }
}
