use crate::{
    block_cache::BlockCache, commit_log::CommitLog, level::Levels, memtable::MemTable, Config,
};
use std::{
    collections::BTreeMap,
    sync::{atomic::AtomicU64, Arc, Mutex, RwLock},
};
use std_semaphore::Semaphore;

pub struct TreeInner {
    pub(crate) config: Config,
    pub(crate) active_memtable: Arc<RwLock<MemTable>>,
    pub(crate) immutable_memtables: Arc<RwLock<BTreeMap<String, Arc<MemTable>>>>,
    pub(crate) commit_log: Arc<Mutex<CommitLog>>,
    pub(crate) block_cache: Arc<BlockCache>,
    pub(crate) lsn: AtomicU64,
    pub(crate) levels: Arc<RwLock<Levels>>,

    pub(crate) flush_semaphore: Arc<Semaphore>,

    pub(crate) compaction_semaphore: Arc<Semaphore>,
}

impl Drop for TreeInner {
    fn drop(&mut self) {
        log::debug!("Dropping TreeInner");

        log::trace!("Trying to flush commit log");
        if let Ok(mut lock) = self.commit_log.lock() {
            if let Err(error) = lock.flush() {
                log::warn!("Failed to flush commit log: {:?}", error);
            }
        }
    }
}
