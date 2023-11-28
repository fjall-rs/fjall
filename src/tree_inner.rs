use crate::{
    block_cache::BlockCache, commit_log::CommitLog, levels::Levels, memtable::MemTable, Config,
};
use std::{
    collections::BTreeMap,
    sync::{atomic::AtomicU64, Arc, Mutex, RwLock},
};
use std_semaphore::Semaphore;

pub struct TreeInner {
    /// Tree configuration
    pub(crate) config: Config,

    /// Last-seen sequence number (highest sequence number)
    pub(crate) lsn: AtomicU64,

    /// Commit log aka Journal aka Write-ahead log (WAL)
    pub(crate) commit_log: Arc<Mutex<CommitLog>>,

    /// Active memtable
    pub(crate) active_memtable: Arc<RwLock<MemTable>>,

    /// Memtables that are being flushed
    pub(crate) immutable_memtables: Arc<RwLock<BTreeMap<String, Arc<MemTable>>>>,

    /// Tree levels that contain segments
    pub(crate) levels: Arc<RwLock<Levels>>,

    /// Concurrent block cache
    pub(crate) block_cache: Arc<BlockCache>,

    /// Semaphore to limit flush threads
    pub(crate) flush_semaphore: Arc<Semaphore>,

    /// Semaphore to notify compaction threads
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
