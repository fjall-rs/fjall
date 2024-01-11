use crate::{
    config::{Config, PersistedConfig},
    descriptor_table::FileDescriptorTable,
    file::LEVELS_MANIFEST_FILE,
    levels::Levels,
    memtable::MemTable,
    snapshot::SnapshotCounter,
    stop_signal::StopSignal,
    BlockCache,
};
use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
};

pub type SealedMemtables = BTreeMap<Arc<str>, Arc<MemTable>>;

pub struct TreeInner {
    /// Active memtable that is being written to
    pub(crate) active_memtable: Arc<RwLock<MemTable>>,

    /// Frozen memtables that are being flushed
    pub(crate) sealed_memtables: Arc<RwLock<SealedMemtables>>,

    /// Levels manifest
    pub(crate) levels: Arc<RwLock<Levels>>,

    /// Tree configuration
    pub config: PersistedConfig,

    /// Block cache
    pub block_cache: Arc<BlockCache>,

    /// File descriptor cache table
    pub descriptor_table: Arc<FileDescriptorTable>,

    /// Keeps track of open snapshots
    pub(crate) open_snapshots: SnapshotCounter,

    /// Compaction may take a while; setting the signal to `true`
    /// will interrupt the compaction and kill the worker.
    pub(crate) stop_signal: StopSignal,
}

impl TreeInner {
    pub fn create_new(config: Config) -> crate::Result<Self> {
        let levels = Levels::create_new(
            config.inner.level_count,
            config.inner.path.join(LEVELS_MANIFEST_FILE),
        )?;

        Ok(Self {
            config: config.inner,
            block_cache: config.block_cache,
            descriptor_table: config.descriptor_table,
            active_memtable: Arc::default(),
            sealed_memtables: Arc::default(),
            levels: Arc::new(RwLock::new(levels)),
            open_snapshots: SnapshotCounter::default(),
            stop_signal: StopSignal::default(),
        })
    }
}

impl Drop for TreeInner {
    fn drop(&mut self) {
        log::debug!("Dropping TreeInner");

        log::trace!("Sending stop signal to compactors");
        self.stop_signal.send();
    }
}
