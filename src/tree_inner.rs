use crate::{
    block_cache::BlockCache,
    journal::{mem_table::MemTable, Journal},
    levels::Levels,
    Config,
};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicU32, AtomicU64},
        Arc, RwLock,
    },
};
use std_semaphore::Semaphore;

pub struct TreeInner {
    /// Tree configuration
    pub(crate) config: Config,

    /// Last-seen sequence number (highest sequence number)
    pub(crate) lsn: AtomicU64,

    /// Approximate memtable size
    /// If this grows to large, a flush is triggered
    pub(crate) approx_memtable_size_bytes: AtomicU32,

    /// Journal aka Commit log aka Write-ahead log (WAL)
    ///
    /// This also contains the active memtable, sharded by journal shard
    pub(crate) journal: Journal,

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

        log::trace!("Trying to flush journal");
        if let Err(error) = self.journal.flush() {
            log::warn!("Failed to flush journal: {:?}", error);
        }
    }
}
