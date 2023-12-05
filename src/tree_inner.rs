use crate::{
    block_cache::BlockCache, journal::Journal, levels::Levels, memtable::MemTable, Config,
};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, AtomicU32, AtomicU64},
        Arc, RwLock,
    },
};
use std_semaphore::Semaphore;

pub struct TreeInner {
    /// Tree configuration
    pub(crate) config: Config,

    /// Last-seen sequence number (highest sequence number)
    // TODO: rename this to "next_seqno" or something, because it's not the highest seqno
    // in the tree, but the highest one + 1
    pub(crate) lsn: AtomicU64,

    // TODO: move into memtable
    /// Approximate active memtable size
    /// If this grows to large, a flush is triggered
    pub(crate) approx_active_memtable_size: AtomicU32,
    //  pub(crate) approx_active_memtable_size: AtomicU32,
    pub(crate) active_memtable: Arc<RwLock<MemTable>>,

    /// Journal aka Commit log aka Write-ahead log (WAL)
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

    /// Keeps track of open snapshots
    pub(crate) open_snapshots: AtomicU32,

    /// Notifies compaction threads that the tree is dropping
    pub(crate) stop_signal: AtomicBool,
}

impl Drop for TreeInner {
    fn drop(&mut self) {
        log::debug!("Dropping TreeInner");

        log::debug!("Trying to flush journal");
        if let Err(error) = self.journal.flush() {
            log::warn!("Failed to flush journal: {:?}", error);
        }

        log::debug!("Sending stop signal to compaction threads");
        self.stop_signal
            .store(true, std::sync::atomic::Ordering::Release);
    }
}
