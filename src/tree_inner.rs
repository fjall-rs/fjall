use crate::{
    block_cache::BlockCache, journal::Journal, partition::Partition, stop_signal::StopSignal,
    Config,
};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU32, AtomicU64},
        Arc, RwLock,
    },
};
use std_semaphore::Semaphore;

pub struct TreeInner {
    /// Tree configuration
    pub(crate) config: Config,

    /// Next sequence number (last sequence number (LSN) + 1)
    pub(crate) next_lsn: Arc<AtomicU64>,

    /// Journal aka Commit log aka Write-ahead log (WAL)
    pub(crate) journal: Arc<Journal>,

    /// Data partitions
    pub(crate) partitions: Arc<RwLock<HashMap<Arc<str>, Partition>>>,

    /// Concurrent block cache
    pub(crate) block_cache: Arc<BlockCache>,

    /// Semaphore to limit flush threads
    pub(crate) flush_semaphore: Arc<Semaphore>,

    /// Semaphore to notify compaction threads
    pub(crate) compaction_semaphore: Arc<Semaphore>,

    /// Keeps track of open snapshots
    pub(crate) open_snapshots: Arc<AtomicU32>,

    /// Notifies compaction threads that the tree is dropping
    pub(crate) stop_signal: StopSignal,
}

impl Drop for TreeInner {
    fn drop(&mut self) {
        log::debug!("Dropping TreeInner");

        log::debug!("Sending stop signal to threads");
        self.stop_signal.send();

        log::debug!("Trying to flush journal");
        if let Err(error) = self.journal.flush() {
            log::warn!("Failed to flush journal: {:?}", error);
        }

        // TODO: spin lock until thread_count reaches 0
    }
}
